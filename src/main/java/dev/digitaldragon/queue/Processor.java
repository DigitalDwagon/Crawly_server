package dev.digitaldragon.queue;

import com.google.common.util.concurrent.RateLimiter;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import dev.digitaldragon.database.mongo.MongoManager;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.*;

public class Processor {
    private static final String URL_FILE_PATH = "asasasas.txt";
    private static Map<String, Boolean> dnsCache = new HashMap<>(); // DNS cache


    public static void process() {
        RateLimiter rateLimiter = RateLimiter.create(1400.0);
        MongoCollection<Document> rejectsCollection = MongoManager.getRejectsCollection();
        MongoCollection<Document> bigQueueCollection = MongoManager.getBigqueueCollection();
        MongoCollection<Document> processingCollection = MongoManager.getProcessingCollection();

        while (processingCollection.countDocuments() > 0) {
            if (bigQueueCollection.countDocuments() > 500000) {
                continue;
            }

            try (MongoCursor<Document> cursor = processingCollection.aggregate(
                    List.of(new Document("$sample", new Document("size", 1)))).iterator()) {

                    if (!cursor.hasNext()) {
                        continue;
                    }

                    Document document = cursor.next();
                    String url = document.get("url").toString();

                if (isDuplicate(url, bigQueueCollection) || isDuplicate(url, rejectsCollection)) {
                    document.append("reject_reason", "DUPLICATE");
                } else if (!isValidURL(url)) {
                    document.append("reject_reason", "INVALID_URL");
                } else if (!hasValidDNS(url, rateLimiter)) {
                    document.append("reject_reason", "INVALID_DNS");
                }

                document.append("processed_at", Instant.now());
                document.remove("_id");

                if (document.get("reject_reason") != null) {
                    document.append("status", "REJECTED");
                    rejectsCollection.insertOne(document);
                } else {
                    document.append("status", "QUEUED");
                    bigQueueCollection.insertOne(document);
                }
            }
        }
    }

    private static boolean isDuplicate(String url, MongoCollection<Document> collection) {
        Bson filter = Filters.eq("url", url);
        return collection.find(filter).cursor().hasNext();
    }

    private static boolean isValidURL(String url) {
        try {
            new URL(url).toURI();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean hasValidDNS(String url, RateLimiter rateLimiter) {
        String hostname = null;
        try {
            hostname = new URL(url).getHost();
        } catch (MalformedURLException e) {
            return false;
        }

        if (dnsCache.get(hostname) != null)
            return dnsCache.get(hostname);

        rateLimiter.acquire();

        try {
            // Perform DNS lookup using a random server
            System.out.printf("Querying for domain %s%n", hostname);
            InetAddress address = InetAddress.getByName(hostname);
            if (address.isAnyLocalAddress()) {
                throw new UnknownHostException(); //probably not the best way to do this but it works lol
            }
        } catch (UnknownHostException e) {
            dnsCache.put(hostname, false);
            System.out.println("No valid DNS.");
            return false; // DNS lookup failed
        }

        dnsCache.put(hostname, true);
        return true;
    }
}
