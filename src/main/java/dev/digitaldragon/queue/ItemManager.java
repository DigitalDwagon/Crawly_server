package dev.digitaldragon.queue;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import dev.digitaldragon.database.Database;
import dev.digitaldragon.database.WriteManager;
import dev.digitaldragon.database.mongo.MongoManager;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.JSONObject;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Time;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.mongodb.client.model.Filters.eq;

public class ItemManager {
    public static void bulkQueueURLs(Set<String> urls, String username) { //TODO read cache
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            // Load all necessary collections
            MongoCollection<Document> queueCollection = MongoManager.getQueueCollection();
            MongoCollection<Document> outCollection = MongoManager.getOutCollection();
            MongoCollection<Document> doneCollection = MongoManager.getDoneCollection();

            // Group URLs by collection to which they belong
            Map<MongoCollection<Document>, List<Document>> documentsByCollection = new HashMap<>();
            for (String url : urls) {
                Database endDatabase = Database.REJECTS;

                try {
                    URI uri = new URI(url);
                    if (uri.getScheme() != null && uri.getHost() != null) {
                        endDatabase = Database.QUEUE;
                    }
                } catch (URISyntaxException e) {
                    //do nothing, already assigned as a reject
                }


                if (duplicationChecker(doneCollection, url) || duplicationChecker(outCollection, url) || duplicationChecker(queueCollection, url)) {
                    endDatabase = Database.DUPLICATES;
                }

                JSONObject write = new JSONObject()
                        .put("url", url)
                        .put("queuedAt", Time.from(Instant.now()).toString())
                        .put("queuedBy", username);

                WriteManager.itemAdd(endDatabase, write);
            }

            // Perform bulk writes for each collection
            for (Map.Entry<MongoCollection<Document>, List<Document>> entry : documentsByCollection.entrySet()) {
                MongoCollection<Document> collection = entry.getKey();
                List<Document> documents = entry.getValue();
                collection.insertMany(documents);
                System.out.println("Processed " + documents.size() + " URLs in " + collection.getNamespace());
            }
        });
    }

     // ---------------------
     // Duplication Checker
     // check if a record for this url already exists
     // ---------------------
     // todo does not account for same urls with or without ending / (eg https://example.com/ is not treated the same as https://example.com)
     private static boolean duplicationChecker(MongoCollection<Document> collection, String url) { //TODO read cache
         Bson filter = Filters.eq("url", url);
         try (MongoCursor<Document> cursor = collection.find(filter).iterator()) {
             return cursor.hasNext();
         }
     }

    public static void submitCrawlInfo(JSONObject data) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {

            // TODO validate provided data
            String url = data.get("url").toString();
            WriteManager.itemAdd(Database.DONE, data);
            WriteManager.itemRemove(Database.QUEUE, url);
            WriteManager.itemRemove(Database.OUT, url);

        });
    }
}
