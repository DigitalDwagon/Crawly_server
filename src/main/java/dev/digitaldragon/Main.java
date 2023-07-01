package dev.digitaldragon;

import dev.digitaldragon.database.ReadManager;
import dev.digitaldragon.database.WriteManager;
import dev.digitaldragon.database.mongo.MongoManager;
import dev.digitaldragon.queue.Processor;
import dev.digitaldragon.database.mongo.QueueMover;
import dev.digitaldragon.database.mongo.UrlsExporter;
import dev.digitaldragon.queue.CrawlManager;
import dev.digitaldragon.queue.ItemManager;
import org.bson.json.JsonObject;
import org.json.JSONArray;
import org.json.JSONObject;
import spark.Spark;

import java.util.*;
import java.util.stream.Collectors;

import static spark.Spark.*;


public class Main {

    public static void main(String[] args) {
        MongoManager.initializeDb();
        ReadManager.refreshCaches(true);


        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "TRACE");
        Spark.port(443);
        try {
            //secure("C:\\Users\\Digital\\Desktop\\dev_archive\\Crawly_server\\certs\\cert.pem", "C:\\Users\\Digital\\Desktop\\dev_archive\\Crawly_server\\certs\\privkey.key", null, null);
            Spark.init();
        } catch (Exception e) {
            e.printStackTrace();
        }


        get("/.well-known/acme-challenge/U4jZOD7td8ZTRwgp1Z6skizUgoZFQRVn46sN-mY5RgQ", (request, response) -> {
            return "U4jZOD7td8ZTRwgp1Z6skizUgoZFQRVn46sN-mY5RgQ.bpJoExtDbf0sv1LhnGRBwh05tWQX8McztNTgwE-rUhI";
        });

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

        get("/queue/domains", (request, response) -> new JsonObject(Map.of("domains", CrawlManager.uniqueDomains()).toString()));

        //TODO release user claim(s) method
        //TODO release global claims older than date method

        post("/queue", (request, response) -> {
            response.type("application/json");
            String username = request.queryParams("username");

            JSONObject jsonObject = new JSONObject(request.body());
            Set<String> InputUrls = jsonObject.getJSONArray("urls").toList().stream().map(Object::toString).collect(Collectors.toSet());

            ItemManager.bulkQueueURLs(InputUrls, username);

            return new JSONObject(Map.of("success", true)).toString();
        });

        post("/queue/move", (request, response) -> {
            response.type("application/json");
            String username = request.queryParams("username");

            JSONObject jsonObject = new JSONObject(request.body());
            QueueMover.move(jsonObject.getInt("amount"));

            return new JSONObject(Map.of("success", true)).toString();
        });


        post("/jobs/submit", (request, response) -> {
            response.type("application/json");


            // grab and parse json
            JSONObject jsonObject = new JSONObject(request.body());
            String crawlerUsername = jsonObject.getString("username");

            // queue up discovered urls
            JSONArray discovered = jsonObject.getJSONArray("discovered");
            Set<String> urls = discovered.toList().stream().map(Object::toString).collect(Collectors.toSet());
            System.out.printf("Sending %s items for deduplication and queuing%n", urls.size());
            ItemManager.bulkQueueURLs(urls, crawlerUsername);

            if (jsonObject.get("ip") != null) {
                jsonObject.put("reported_ip", jsonObject.get("ip"));
            }
            jsonObject.put("ip", request.ip());

            // submit finished url to done
            ItemManager.submitCrawlInfo(jsonObject);

            return new JSONObject(Map.of("status", "ok")).toString();
        });

        post("/jobs/abort", (request, response) -> {
            response.type("application/json");

            JSONObject jsonObject = new JSONObject(request.body());
            String url = jsonObject.get("url").toString();

            //todo finish this
            return null;
        });

        get("/admin/test", (request, response) -> {
            int amount = Integer.parseInt(request.queryParams("amount"));

            Set<String> urls = ReadManager.itemGetUniqueDomainUrls(amount);

            if (urls.isEmpty()) {
                response.status(500);
                return new JSONObject(Map.of("error", "queue empty or error fetching from queue")).toString();
            }

            JSONArray urlsJsonArray = new JSONArray(urls);

            JSONObject responseJson = new JSONObject();
            responseJson.put("urls", urlsJsonArray);

            return responseJson;
        });

        get("/whoami", (request, response) -> {
            for (String s : request.headers()) {
                System.out.println(s);
            }
            JSONObject responseJson = new JSONObject();
            responseJson.put("success", "true");

            return responseJson;
        });

        Spark.awaitInitialization();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Flushing writes...");
            WriteManager.flush(true);
            System.out.println("Writes flushed.");
        }));

        post("/admin/cacheupdate", (request, response) -> {
            WriteManager.flush(true);
            ReadManager.refreshCaches(true);
            return new JSONObject(Map.of("success", "true")).toString();
        });

        post("/admin/export", ((request, response) -> {
            UrlsExporter.export(List.of(
                    MongoManager.getBigqueueCollection(),
                    MongoManager.getDoneCollection(),
                    MongoManager.getOutCollection(),
                    MongoManager.getDuplicatesCollection(),
                    MongoManager.getQueueCollection(),
                    MongoManager.getProcessingCollection(),
                    MongoManager.getRejectsCollection()
            ));
            return new JSONObject(Map.of("success", "true")).toString();
        }));
    }
}