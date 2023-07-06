package dev.digitaldragon;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import dev.digitaldragon.database.Database;
import dev.digitaldragon.database.ReadManager;
import dev.digitaldragon.database.WriteManager;
import dev.digitaldragon.database.mongo.MongoManager;
import dev.digitaldragon.database.mongo.QueueMover;
import dev.digitaldragon.database.mongo.UrlsExporter;
import dev.digitaldragon.queue.CrawlManager;
import dev.digitaldragon.queue.ItemManager;
import dev.digitaldragon.queue.Mover;
import dev.digitaldragon.queue.Processor;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonObject;
import org.eclipse.jetty.util.ajax.JSON;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import spark.Response;
import spark.Spark;

import java.time.DateTimeException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static spark.Spark.*;


public class Main {

    public static void main(String[] args) {
        MongoManager.initializeDb();
        ReadManager.refreshCaches(true);
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "TRACE");
        Spark.port(4567);
        Spark.init();

        ExecutorService executor = Executors.newFixedThreadPool(2);
        executor.submit(Processor::process);
        executor.submit(Mover::move);


        // Define a before filter to validate the username
        before("/jobs", (request, response) -> {
            String username = request.queryParams("username");
            if (username == null || username.isEmpty()) {
                response.status(400);
                response.body(new JSONObject(Map.of("error", "usernames are required on this route")).toString());
                halt();
            }
        });

        get("/jobs/queue", (request, response) -> {
            response.type("application/json");
            String username = request.queryParams("username");
            System.out.println(request.queryParams("amount"));

            if (username == null) {
                return createErrorResponse(response, "USERNAME_REQUIRED", 400);
            }

            final int DEFAULT_AMOUNT = 1;
            final int MIN_AMOUNT = 1;
            final int MAX_AMOUNT = 2000;

            int amount = DEFAULT_AMOUNT;
            try {
                amount = Integer.parseInt(request.queryParams("amount"));
                amount = Math.min(Math.max(amount, MIN_AMOUNT), MAX_AMOUNT);
            } catch (NumberFormatException e) {
                // Ignore exception and use default amount
            }

            List<String> urls = CrawlManager.getUniqueDomainUrls(amount, username);

            if (urls.isEmpty()) {
                return createErrorResponse(response, "NO_JOBS", 500);
            }

            JSONArray urlsJsonArray = new JSONArray(urls);

            JSONObject responseJson = new JSONObject()
                    .put("urls", urlsJsonArray);

            return responseJson.toString();
        });

        get("/jobs/queue/domains/count", (request, response) -> new JSONObject(Map.of("domains", CrawlManager.queueUniqueDomains().size())));

        get("/jobs/queue/domains", (request, response) -> {
            JSONObject responseJson = new JSONObject();
            JSONArray domains = new JSONArray();
            //for (String element : CrawlManager.queueUniqueDomains()) {
            //    domains.put(element);
            //}
            responseJson.put("domains", CrawlManager.queueUniqueDomains());
            return responseJson;
        });

        post("/jobs/done/requeue", (request, response) -> {
            response.type("application/json");
            String username = request.queryParams("username");

            if (username == null) {
                return createErrorResponse(response, "USERNAME_REQUIRED", 400);
            }

            JSONObject requestJson = new JSONObject(request.body());
            MongoCollection<Document> doneCollection = MongoManager.getDoneCollection();

            for (Object element : requestJson.getJSONArray("urls")) {
                String url = element.toString();
                Set<String> urls = new HashSet<>();
                urls.add(url);


                Bson filter = Filters.eq("url", url);
                Document document = doneCollection.findOneAndDelete(filter);
                if (document == null) {
                    continue;
                }
                Document crawl = (Document) document.get("crawl");

                // Check if discovered_outlinks exists in crawl object
                if (crawl.containsKey("discovered_outlinks")) {
                    ArrayList<String> outlinks = ((List<?>) crawl.get("discovered_outlinks")).stream().map(Object::toString).collect(Collectors.toCollection(ArrayList::new));
                    urls.addAll(outlinks);
                }

                // Check if discovered_embeds exists in crawl object
                if (crawl.containsKey("discovered_embeds")) {
                    ArrayList<String> embeds = ((List<?>) crawl.get("discovered_embeds")).stream().map(Object::toString).collect(Collectors.toCollection(ArrayList::new));
                    urls.addAll(embeds);
                }


                System.out.println(urls);
                ItemManager.bulkQueueURLs(urls, username, "REQUEUED_BY_USER");
            }

            return new JSONObject(Map.of("success", true));

        });

        post("/jobs/claims/release", (request, response) -> {
            response.type("application/json");

            //todo move this logic elsewhere
            MongoCollection<Document> outCollection = MongoManager.getOutCollection();
            JSONObject jsonObject = new JSONObject(request.body());
            if (jsonObject.has("urls"))
                for (Object object : jsonObject.getJSONArray("urls")) {
                    String url = object.toString().strip();
                    Bson filter = Filters.eq("url", url);
                    Document document = outCollection.findOneAndDelete(filter);
                    if (document == null) {
                        continue;
                    }
                    document.remove("out_for");
                    document.remove("out_at");
                    WriteManager.itemAdd(Database.BIGQUEUE, document);
                }

            if (jsonObject.getBoolean("all") && jsonObject.getString("username") != null) {
                try (MongoCursor<Document> cursor = outCollection.find().iterator()) {
                    while (cursor.hasNext()) {
                        Document document = cursor.next();
                        if (document.get("out_for") == null) {
                            continue;
                        }
                        if (document.get("out_for").toString().trim().equals(jsonObject.getString("username").trim())) {
                            outCollection.findOneAndDelete(document);
                            document.remove("out_for");
                            document.remove("out_at");
                            WriteManager.itemAdd(Database.BIGQUEUE, document);
                        }
                    }
                }
            }
            return new JSONObject(Map.of("success", true)).toString();
        });

        post("/jobs/queue", (request, response) -> {
            response.type("application/json");
            String username = request.queryParams("username");

            JSONObject jsonObject = new JSONObject(request.body());
            Set<String> InputUrls = jsonObject.getJSONArray("urls").toList().stream().map(Object::toString).collect(Collectors.toSet());

            ItemManager.bulkQueueURLs(InputUrls, username, "SUBMITTED");

            return new JSONObject(Map.of("success", true)).toString();
        });

        /*post("/jobs/queue/move", (request, response) -> {
            response.type("application/json");
            String username = request.queryParams("username");

            JSONObject jsonObject = new JSONObject(request.body());
            Set<String> set = new HashSet<>();
            JSONArray jsonArray = jsonObject.getJSONArray("urls");
            for (Object object : jsonArray){
                set.add(object.toString());
            }
            ItemManager.bulkQueueURLs(set, username, "SUBMITTED");
            return new JSONObject(Map.of("success", true)).toString();
        });*/


        post("/jobs/submit", (request, response) -> {
            response.type("application/json");
            JSONObject requestJson = new JSONObject(request.body());

            try {
                // Attempt to submit the crawl data to the database
                requestJson.put("crawl_submission_ip", request.ip());
                ItemManager.submitCrawlInfo(requestJson);
            } catch (JSONException | NullPointerException exception) {
                return createErrorResponse(response, "BAD_DATA", 400);
            } catch (DateTimeException exception) {
                return createErrorResponse(response, "INVALID_TIME", 400);
            }

            String crawlerUsername = requestJson.getString("username");

            // Queue up discovered outlinks
            JSONArray outlinks = requestJson.getJSONArray("discovered_outlinks");
            System.out.println(outlinks);
            System.out.println(convertJsonArrayToSet(outlinks));
            Set<String> outlinkUrls = convertJsonArrayToSet(outlinks);
            System.out.printf("Sending %s items for deduplication and queuing%n", outlinkUrls.size());
            ItemManager.bulkQueueURLs(outlinkUrls, crawlerUsername, "DISCOVERED_OUTLINKS");

            // Do the same for discovered page embeds (e.g., images)
            JSONArray elements = requestJson.getJSONArray("discovered_embeds");
            System.out.println(elements);
            System.out.println(convertJsonArrayToSet(elements));
            Set<String> elementUrls = convertJsonArrayToSet(elements);
            System.out.printf("Sending %s items for deduplication and queuing%n", elementUrls.size());
            ItemManager.bulkQueueURLs(elementUrls, crawlerUsername, "DISCOVERED_EMBEDS");


            return new JSONObject(Map.of("success", true)).toString();
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

    private static Object createErrorResponse(Response response, String error, int code) {
        response.status(code);
        return new JSONObject(Map.of("success", false)).append("error", error);
    }

    private static Set<String> convertJsonArrayToSet(JSONArray jsonArray) {
        return jsonArray.toList().stream()
                .map(Object::toString)
                .collect(Collectors.toSet());
    }
}