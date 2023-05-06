package dev.digitaldragon.queue;

import com.mongodb.*;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.TransactionBody;
import com.mongodb.client.model.*;
import dev.digitaldragon.database.mongo.MongoManager;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.*;

public class CrawlManager {

    public static List<String> getUniqueDomainUrls(int numUrls, String user) {
        MongoCollection<Document> queueCollection = MongoManager.getQueueCollection();
        MongoCollection<Document> outCollection = MongoManager.getOutCollection();

        Set<String> uniqueDomains = new HashSet<>();
        List<String> urls = new ArrayList<>();

        int maxRetries = 20; // maximum number of retries
        int retries = 0;


        List<WriteModel<Document>> queueOperations = new ArrayList<>();
        List<WriteModel<Document>> outOperations = new ArrayList<>();

        while (urls.size() < numUrls && retries < maxRetries) {
            int fetchSize = Math.min(numUrls - urls.size(), 100); // fetch up to 100 documents at a time
            List<Document> documents = queueCollection.aggregate(Arrays.asList(
                    Aggregates.sample(fetchSize)
            )).into(new ArrayList<>());

            if (documents.isEmpty()) {
                break;
            }

            for (Document document : documents) {
                String url = document.get("url").toString();
                String domain = getDomainFromUrl(url);

                if (!uniqueDomains.contains(domain)) {
                    urls.add(url);
                    uniqueDomains.add(domain);
                    queueOperations.add(new DeleteOneModel<>(document));
                    document.append("claimedBy", user);
                    document.append("claimedAt", Instant.now().toString());
                    outOperations.add(new InsertOneModel<>(document));
                }
            }
            retries++;
        }

        if (!queueOperations.isEmpty()) {
            try {
                queueCollection.bulkWrite(queueOperations);
            } catch (MongoException e) {
                e.printStackTrace();
            }
        }
        if (!outOperations.isEmpty()) {
            outCollection.bulkWrite(outOperations);
        }

        if (urls.size() < numUrls) {
            System.out.println("Could not find enough unique domain URLs after " + retries + " retries");
        } else {
            System.out.println("Queued " + urls.size() + " unique domain URLs after " + retries + " retries");
        }

        return urls;
    }



    public static String getDomainFromUrl(String url) { //todo move it to a nicer class and update it to look nicer
        try {
            URI uri = new URI(url);
            String domain = uri.getHost();
            if (domain != null) {
                return domain.startsWith("www.") ? domain.substring(4) : domain;
            }
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return null;
    }

}
