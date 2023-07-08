package dev.digitaldragon.database;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Projections;
import dev.digitaldragon.database.mongo.MongoManager;
import dev.digitaldragon.queue.CrawlManager;
import org.bson.Document;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
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

    public static String queueGetUrl() {
        refreshCaches();

        Set<String> keySet = queueUrlCache.keySet();
        try {
            String randomKey = keySet.stream()
                    .skip(ThreadLocalRandom.current().nextInt(keySet.size()))
                    .findFirst().orElse(null);
            if (randomKey != null) {
                String url = queueUrlCache.get(randomKey).get(ThreadLocalRandom.current().nextInt(queueUrlCache.get(randomKey).size()));
                cacheRemoveItem(url, queueUrlCache);
                return url;
            }
        } catch (IllegalArgumentException exception) {
            //uh oh;
        }
        return null;
    }

    public static boolean cacheCheckDuplication(String url) {
        String domain = CrawlManager.getDomainFromUrl(url);

        if (domain == null) {
            return false;
        }

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
        if (domain == null) {
            return;
        }
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
        cache.clear();

        while (cache.size() < 7000) {
            try (MongoCursor<Document> cursor = collection.aggregate(
                    List.of(new Document("$sample", new Document("size", 2000)))).iterator()) {
                while (cursor.hasNext() && cache.size() < 20000) {
                    Document document = cursor.next();
                    String url = document.get("url").toString();
                    String domain = CrawlManager.getDomainFromUrl(url);

                    if (domain == null) {
                        continue;
                    }
                    System.out.println(domain);
                    System.out.println(url);
                    System.out.println(cache.size());
                    System.out.println(cache.containsKey(domain));

                    if (!cache.containsKey(domain)) {
                        cache.put(domain, new ArrayList<>());
                    }

                    List<String> domainList = cache.get(domain);
                    if (!domainList.contains(url)) {
                        domainList.add(url);
                    }
                }
            } catch (MongoException e) {
                e.printStackTrace();
            }
        }


        lastRefresh = Instant.now();

        for (Map.Entry<String, List<String>> entry : queueUrlCache.entrySet()) {
            System.out.println(entry.getKey());
            for (String s : entry.getValue()) {
                System.out.println(s);
            }
        }
    }

    public static void refreshCaches(boolean force) {
        if ( !force && queueUrlCache.size() > 5000) {
            System.out.println("skipping refresh");
            return;
        }
        if (lastRefresh.isAfter(Instant.now().minus(30, ChronoUnit.SECONDS))) {
            System.out.println("too close, skipping refresh");
            return;
        }
        MongoCollection<Document> queueCollection = MongoManager.getQueueCollection();
        ExecutorService executor = Executors.newFixedThreadPool(1);

        executor.submit(() -> {
            refreshCaches(queueCollection, queueUrlCache);
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
