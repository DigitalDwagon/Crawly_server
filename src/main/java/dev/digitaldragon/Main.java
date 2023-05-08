package dev.digitaldragon;

import dev.digitaldragon.database.ReadManager;
import dev.digitaldragon.database.WriteManager;
import dev.digitaldragon.database.mongo.MongoManager;
import dev.digitaldragon.queue.CrawlManager;
import dev.digitaldragon.queue.ItemManager;
import org.bson.json.JsonObject;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Spark;

import java.util.*;
import java.util.logging.Level;
import java.util.stream.Collectors;

import static spark.Spark.*;


public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        MongoManager.initializeDb();
        ReadManager.refreshCaches(true);

        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "TRACE");
        Spark.port(1234);
        Spark.init();

        // Define a before filter to validate the username
        before("/queue", (request, response) -> {
            String username = request.queryParams("username");
            if (username == null || username.isEmpty()) {
                response.status(400);
                response.body(new JSONObject(Map.of("error", "usernames are required on this route")).toString());
                halt();
            }
        });

        get("/queue", (request, response) -> {
            response.type("application/json");
            String username = request.queryParams("username");

            int amount = 1;
            try {
                amount = Integer.parseInt(request.queryParams("amount"));
                amount = Math.min(Math.max(amount, 1), 2000);
            } catch (NumberFormatException e) {
                // Ignore exception and use default amount of 1
            }

            List<String> urls = CrawlManager.getUniqueDomainUrls(amount, username);

            if (urls.isEmpty()) {
                response.status(500);
                return new JSONObject(Map.of("error", "queue empty or error fetching from queue")).toString();
            }

            JSONArray urlsJsonArray = new JSONArray(urls);

            JSONObject responseJson = new JSONObject();
            responseJson.put("urls", urlsJsonArray);

            return responseJson.toString();
        });

        get("/queue/domains", (request, response) -> {
            return new JsonObject(Map.of("domains", CrawlManager.uniqueDomains()).toString());
        });

        //TODO release user claim(s) method
        //TODO release global claims older than date method


        //TODO NOTE FOR WHEN I WAKE UP: working on implementing a write cache and read cache (write cache basically done but methods need updating, ReadManager still needs to be created)
        //the goal is that we always read/write from cache and let the methods update the caches as time goes on
        //right now WriteManager batches ~5000 writes per collection and sends them all at once to Mongo. Hopefully this + a read cache will ease load
        //WriteManager has a temporary proxy method for the manual bulk writes being done as a temporary measure before rewriting
        //current suspect for all the load is the deduplicater, so need to cache reads on that and maybe figure out another speed improvement (maybe search through domains first, then urls on that domain?)

        post("/queue", (request, response) -> {
            response.type("application/json");
            String username = request.queryParams("username");

            JSONObject jsonObject = new JSONObject(request.body());
            Set<String> InputUrls = jsonObject.getJSONArray("urls").toList().stream().map(Object::toString).collect(Collectors.toSet());

            ItemManager.bulkQueueURLs(InputUrls, username);

            return new JSONObject(Map.of("success", true)).toString();
        });


        post("/submit", (request, response) -> {
            response.type("application/json");


            // grab and parse json
            JSONObject jsonObject = new JSONObject(request.body());
            String crawlerUsername = jsonObject.getString("username");

            // queue up discovered urls
            JSONArray discovered = jsonObject.getJSONArray("discovered");
            Set<String> urls = discovered.toList().stream().map(Object::toString).collect(Collectors.toSet());
            System.out.printf("Sending %s items for deduplication and queuing%n", urls.size());
            ItemManager.bulkQueueURLs(urls, crawlerUsername);

            // submit finished url to done
            ItemManager.submitCrawlInfo(jsonObject);

            return new JSONObject(Map.of("status", "ok")).toString();
        });

        get("/admin/test", (request, response) -> {
            int amount = Integer.parseInt(request.queryParams("amount"));

            Set<String> urls = ReadManager.itemGetUniqueDomainUrls(10);

            if (urls.isEmpty()) {
                response.status(500);
                return new JSONObject(Map.of("error", "queue empty or error fetching from queue")).toString();
            }

            JSONArray urlsJsonArray = new JSONArray(urls);

            JSONObject responseJson = new JSONObject();
            responseJson.put("urls", urlsJsonArray);

            return responseJson;
        });

        Spark.awaitInitialization();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Flushing writes...");
            WriteManager.flush(true);
            System.out.println("Writes flushed.");
        }));
    }
}