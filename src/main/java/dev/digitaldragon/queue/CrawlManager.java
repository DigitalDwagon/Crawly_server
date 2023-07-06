package dev.digitaldragon.queue;

import com.mongodb.client.MongoCollection;
import dev.digitaldragon.database.ReadManager;
import dev.digitaldragon.database.WriteManager;
import dev.digitaldragon.database.mongo.MongoManager;
import org.bson.Document;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CrawlManager {
    public static List<String> getUniqueDomainUrls(int numUrls, String username) { //TODO rewrite to use read and write caches
        Set<String> uniqueDomains = new HashSet<>();
        List<String> urls = new ArrayList<>();

        int maxRetries = numUrls * 3; // maximum number of retries
        int retries = 0;

        while (urls.size() < numUrls && retries < maxRetries) {
            String url = ReadManager.queueGetUrl();
            if (url == null) {
                retries++;
                continue;
            }
            String domain = getDomainFromUrl(url);

            if (!uniqueDomains.contains(domain)) {
                urls.add(url);
                uniqueDomains.add(domain);
                WriteManager.checkoutQueueItem(username, url);
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

    public static Set<String> queueUniqueDomains() {
        MongoCollection<Document> collection = MongoManager.getQueueCollection(); //todo also count bigqueue
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

        return uniqueDomains;
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
        } catch (URISyntaxException | NullPointerException e) {
            //do nothing lol
        }
        return null;
    }
}
