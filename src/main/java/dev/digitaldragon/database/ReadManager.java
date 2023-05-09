package dev.digitaldragon.database;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Projections;
import dev.digitaldragon.database.mongo.MongoManager;
import dev.digitaldragon.queue.CrawlManager;
import org.bson.Document;
import org.slf4j.LoggerFactory;

import java.sql.Time;
import java.time.Instant;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;

public class ReadManager {
    private static Instant lastRefresh = Instant.EPOCH;
    public static Map<String, List<String>> queueUrlCache = new ConcurrentHashMap<>();
    public static Map<String, List<String>> outUrlCache = new ConcurrentHashMap<>();
    public static Map<String, List<String>> doneUrlCache = new ConcurrentHashMap<>();

    public static Set<String> itemGetUniqueDomainUrls(int number) {
        refreshCaches();
        Set<String> keySet = queueUrlCache.keySet();
        Set<String> urls = new HashSet<>();
        int maxRetries = number * 2;
        int retries = 0;

        while (urls.size() < number && retries < maxRetries && !keySet.isEmpty()) {
            String randomKey = keySet.stream()
                    .skip(ThreadLocalRandom.current().nextInt(keySet.size()))
                    .findFirst().orElse(null);

            if (randomKey != null) {
                List<String> domainItem = queueUrlCache.get(randomKey);
                if (domainItem.isEmpty()) {
                    retries++;
                    continue;
                }
                String item = domainItem.get(0);
                urls.add(item);
            }
            retries++;
        }
        return urls;
    }

    public static boolean cacheCheckDuplication(String url) {
        String domain = CrawlManager.getDomainFromUrl(url);

        if (doneUrlCache.get(domain).contains(url)) {
            return true;
        }
        if (outUrlCache.get(domain).contains(url)) {
            return true;
        }
        return queueUrlCache.get(domain).contains(url);
    }

    public static void cacheRemoveItem(String url, Map<String, List<String>> cache) {
        String domain = CrawlManager.getDomainFromUrl(url);
        cache.get(domain).remove(url);
        if (cache.get(domain).isEmpty()) {
            cache.remove(domain);
        }
    }

    public static void cacheAddItem(String url, Map<String, List<String>> cache) {
        String domain = CrawlManager.getDomainFromUrl(url);
        if (!cache.containsKey(domain)) {
            cache.put(domain, new ArrayList<>());
        }

        cache.get(domain).add(url);
    }

    public static void cacheAddItem(String url, Database database) {
        switch (database) {
            case QUEUE -> cacheAddItem(url, queueUrlCache);
            case OUT -> cacheAddItem(url, outUrlCache);
            case DONE -> cacheAddItem(url, doneUrlCache);
        }
    }

    public static void cacheRemoveItem(String url, Database database) {
        switch (database) {
            case QUEUE -> cacheRemoveItem(url, queueUrlCache);
            case OUT -> cacheRemoveItem(url, outUrlCache);
            case DONE -> cacheRemoveItem(url, doneUrlCache);
        }
    }



    private static void refreshCaches(MongoCollection<Document> collection, Map<String, List<String>> cache) {
        System.out.printf("Rebuilding cache %s%n", collection.getNamespace());

        List<String> fieldsToRetrieve = Collections.singletonList("url");
        try (MongoCursor<Document> cursor = collection.find().projection(Projections.include(fieldsToRetrieve)).batchSize(1000).iterator()) {
            while (cursor.hasNext()) {
                Document document = cursor.next();
                String url = document.get("url").toString();
                String domain = CrawlManager.getDomainFromUrl(url);

                if (!cache.containsKey(domain)) {
                    cache.put(domain, new ArrayList<>());
                }

                cache.get(domain).add(url);
            }
        } catch (MongoException e) {
            e.printStackTrace();
        }
        lastRefresh = Instant.now();
        System.out.printf("Finished rebuilding cache %s%n", collection.getNamespace());
    }

    public static void refreshCaches(boolean force) {
        Instant now = Instant.now();
            Instant time15MinAgo = now.minus(10, ChronoUnit.MINUTES);
        if (!lastRefresh.isBefore(Instant.from(time15MinAgo)) && !force) {
            return;
        }
        System.out.println("Refreshing read caches...");
        MongoCollection<Document> queueCollection = MongoManager.getQueueCollection();
        MongoCollection<Document> outCollection = MongoManager.getOutCollection();
        MongoCollection<Document> doneCollection = MongoManager.getDoneCollection();
        ExecutorService executor = Executors.newFixedThreadPool(1);

        executor.submit(() -> {
            refreshCaches(queueCollection, queueUrlCache);
        });

        executor.submit(() -> {
            refreshCaches(outCollection, outUrlCache);
        });

        executor.submit(() -> {
            refreshCaches(doneCollection, doneUrlCache);
        });

        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public static void refreshCaches() {
        refreshCaches(false);
    }
}
