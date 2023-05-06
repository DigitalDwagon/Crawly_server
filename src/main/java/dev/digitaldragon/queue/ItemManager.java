package dev.digitaldragon.queue;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.result.DeleteResult;
import dev.digitaldragon.database.mongo.MongoManager;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.JSONObject;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Time;
import java.time.Instant;
import java.util.*;

import static com.mongodb.client.model.Filters.eq;

public class ItemManager {
    // ---------------------
    // Submits a url to the mongo queue
    public static void queueURL(String url, String username) {

        //Yes, current shitty coding does require loading EVERY COLLECTION :(
        MongoCollection<Document> queueCollection = MongoManager.getQueueCollection();
        MongoCollection<Document> outCollection = MongoManager.getOutCollection();
        MongoCollection<Document> doneCollection = MongoManager.getDoneCollection();
        MongoCollection<Document> duplicatesCollection = MongoManager.getDuplicatesCollection();
        MongoCollection<Document> rejectsCollection = MongoManager.getRejectsCollection();
        MongoCollection<Document> endCollection = queueCollection; //Used to determine where a URL goes.
        try {
            URI uri = new URI(url);
            if (uri.getScheme() != null && uri.getHost() != null) {
                endCollection = queueCollection;
            } else {
                endCollection = rejectsCollection;
            }
        } catch (URISyntaxException e) {
            endCollection = rejectsCollection;
        } //Check if the URL is valid

        //check if the URL is already in db
        if (duplicationChecker(queueCollection, url) | duplicationChecker(doneCollection, url) | duplicationChecker(outCollection, url)) { //todo this shouldnt be like this
            endCollection = duplicatesCollection;
        }


        Document document = new Document("url", url);
        document.append("queuedAt", Time.from(Instant.now()).toString());
        document.append("queuedBy", username);
        endCollection.insertOne(document);
        System.out.println("Processed: " + url + " (" + endCollection.getNamespace() + ")");
    }


    public static void bulkQueueURLs(Set<String> urls, String username) {
        // load all necessary collections
        MongoCollection<Document> queueCollection = MongoManager.getQueueCollection();
        MongoCollection<Document> outCollection = MongoManager.getOutCollection();
        MongoCollection<Document> doneCollection = MongoManager.getDoneCollection();
        MongoCollection<Document> duplicatesCollection = MongoManager.getDuplicatesCollection();
        MongoCollection<Document> rejectsCollection = MongoManager.getRejectsCollection();

        // group URLs by collection to which they belong
        Map<MongoCollection<Document>, List<Document>> documentsByCollection = new HashMap<>();
        for (String url : urls) {
            MongoCollection<Document> endCollection = queueCollection;
            try {
                URI uri = new URI(url);
                if (uri.getScheme() != null && uri.getHost() != null) {
                    endCollection = queueCollection;
                } else {
                    endCollection = rejectsCollection;
                }
            } catch (URISyntaxException e) {
                endCollection = rejectsCollection;
            }
            if (duplicationChecker(queueCollection, url) ||
                    duplicationChecker(doneCollection, url) ||
                    duplicationChecker(outCollection, url)) {
                endCollection = duplicatesCollection;
            }
            Document document = new Document("url", url);
            document.append("queuedAt", Time.from(Instant.now()).toString());
            document.append("queuedBy", username);
            documentsByCollection.computeIfAbsent(endCollection, k -> new ArrayList<>()).add(document);
        }

        // perform bulk writes for each collection
        for (Map.Entry<MongoCollection<Document>, List<Document>> entry : documentsByCollection.entrySet()) {
            MongoCollection<Document> collection = entry.getKey();
            List<Document> documents = entry.getValue();
            collection.insertMany(documents);
            System.out.println("Processed " + documents.size() + " URLs in " + collection.getNamespace());
        }
    }


     // ---------------------
     // Duplication Checker
     // check if a record for this url already exists
     // ---------------------
     // todo does not account for same urls with or without ending / (eg https://example.com/ is not treated the same as https://example.com)
    private static boolean duplicationChecker(MongoCollection<Document> collection, String url) {
        Bson filter = eq("url", url);
        long count = collection.countDocuments(filter);
        return count > 0;
    }

    public static void submitCrawlInfo(JSONObject data) {
        MongoCollection<Document> queueCollection = MongoManager.getQueueCollection();
        MongoCollection<Document> outCollection = MongoManager.getOutCollection();
        MongoCollection<Document> doneCollection = MongoManager.getDoneCollection();

        Document document = Document.parse(data.toString());
        doneCollection.insertOne(document);

        //search for and remove from queue and out
        Bson filter = eq("url", data.get("url"));
        DeleteResult outResult = outCollection.deleteMany(filter);
        DeleteResult queueResult = queueCollection.deleteMany(filter);
        if (outResult.getDeletedCount() > 0) {
            System.out.println("Released " + outResult.getDeletedCount() + " holds for " + data.get("url"));
        }
        if (queueResult.getDeletedCount() > 0) {
            System.out.println("Removed " + queueResult.getDeletedCount() + " queue items for " + data.get("url"));
        }
    }

    public static String getQueuedURL() {
        MongoCollection<Document> queueCollection = MongoManager.getQueueCollection();
        MongoCollection<Document> outCollection = MongoManager.getOutCollection();

        Document document = queueCollection.aggregate(Arrays.asList(Aggregates.sample(1))).first();

        if (document == null) { return null; }

        queueCollection.deleteOne(document);
        outCollection.insertOne(document);

        System.out.println("Queued: " + document.get("url"));
        return document.get("url").toString(); //todo shouldnt have to tostring a string
    }

    private static boolean isRatelimited(String ip, String domain) {




        return false;
    }


}
