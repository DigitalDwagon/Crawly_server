package dev.digitaldragon;

import dev.digitaldragon.database.WriteManager;
import dev.digitaldragon.database.mongo.MongoManager;
import dev.digitaldragon.queue.CrawlManager;
import dev.digitaldragon.queue.ItemManager;
import org.bson.json.JsonObject;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;
import java.util.stream.Collectors;

import static spark.Spark.*;


public class Main {
    public static void main(String[] args) {
        MongoManager.initializeDb();

        // Define a before filter to validate the username
        before((request, response) -> {
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

        post("/queue", (request, response) -> {
            response.type("application/json");
            String username = validateUsername(request.queryParams("username"));

            JSONObject jsonObject = new JSONObject(request.body());
            Set<String> InputUrls = jsonObject.getJSONArray("urls").toList().stream().map(Object::toString).collect(Collectors.toSet());

            ItemManager.bulkQueueURLs(InputUrls, username);

            return new JSONObject(Map.of("success", true)).toString();
        });


        post("/submit", (request, response) ->  {
            response.type("application/json");
            String username = validateUsername(request.queryParams("username"));

            // grab and parse json
            JSONObject jsonObject = new JSONObject(request.body());
            String crawlerUsername = jsonObject.getString("username");

            // queue up discovered urls
            JSONArray discovered = jsonObject.getJSONArray("discovered");
            Set<String> urls = discovered.toList().stream().map(Object::toString).collect(Collectors.toSet());
            System.out.printf("Sending %s items for deduplication and queuing%n", urls.size());
            ItemManager.bulkQueueURLs(urls, username);

            // submit finished url to done
            ItemManager.submitCrawlInfo(jsonObject);

            return new JSONObject(Map.of("status", "ok")).toString();
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Flushing writes...");
            WriteManager.flush(true);
            System.out.println("Writes flushed.");
        }));
    }
    private static String validateUsername(String username) {
        if (username == null || username.isEmpty()) {
            throw new IllegalArgumentException("Username is required");
        }
        return username;
    }
}