package dev.digitaldragon.queue;

import com.mongodb.*;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import dev.digitaldragon.database.Database;
import dev.digitaldragon.database.ReadManager;
import dev.digitaldragon.database.WriteManager;
import dev.digitaldragon.database.mongo.MongoManager;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.JSONObject;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.*;

public class CrawlManager {
    public static List<String> getUniqueDomainUrls(int numUrls, String user) { //TODO rewrite to use read and write caches
        Set<String> uniqueDomains = new HashSet<>();
        List<String> urls = new ArrayList<>();

        int maxRetries = 150; // maximum number of retries
        int retries = 0;

        while (urls.size() < numUrls && retries < maxRetries) {
            int fetchSize = Math.min(numUrls - urls.size(), 1000); // fetch up to 1000 documents at a time
            Set<String> items = ReadManager.itemGetUniqueDomainUrls(fetchSize);

            for (String url : items) {
                String domain = getDomainFromUrl(url);

                if (!uniqueDomains.contains(domain)) {
                    urls.add(url);
                    uniqueDomains.add(domain);
                    ReadManager.cacheAddItem(url, Database.OUT);
                    ReadManager.cacheRemoveItem(url, Database.QUEUE);
                    WriteManager.itemRemove(Database.QUEUE, url);
                    JSONObject jsonObject = new JSONObject()
                            .put("url", url)
                            .put("claimedBy", user)
                            .put("claimedAt", Instant.now().toString());
                    WriteManager.itemAdd(Database.OUT, jsonObject);
                }
            }
            retries++;
        }

        if (urls.size() < numUrls) {
            System.out.println("Could not find enough unique domain URLs after " + retries + " retries");
        } else {
            System.out.println("Sent " + urls.size() + " unique domain URLs after " + retries + " retries");
        }

        return urls;
    }

    public static long uniqueDomains() {
        MongoCollection<Document> collection = MongoManager.getQueueCollection();
        long totalDocuments = collection.countDocuments();
        int batchSize = 50;

        // Loop through documents in batches
        int offset = 0;

        Set<String> uniqueDomains = new HashSet<>();
        try {
            List<Document> batch = new ArrayList<>();
            while (offset < totalDocuments) {
                batch.addAll(collection.find().skip(offset).limit(batchSize).into(new ArrayList<>()));
                offset = offset + batchSize;
            }
            for (Document doc : batch) {
                String domain = getDomainFromUrl(doc.get("url", String.class));
                uniqueDomains.add(domain);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        for (String domain : uniqueDomains) {
            System.out.println(domain);
        }

        return uniqueDomains.size();
    }



    public static String getDomainFromUrl(String url) {
        try {
            URI uri = new URI(url);
            String host = uri.getHost();
            if (host != null) {
                String[] parts = host.split("\\.");
                if (parts.length > 2 && parts[parts.length - 2].equals("co")) {
                    return parts[parts.length - 3] + "." + parts[parts.length - 2] + "." + parts[parts.length - 1];
                } else if (parts.length < 2) {
                    return url;
                } else {
                    return parts[parts.length - 2] + "." + parts[parts.length - 1];
                }
            }
        } catch (URISyntaxException e) {
            //do nothing lol
        }
        return null;
    }
}
