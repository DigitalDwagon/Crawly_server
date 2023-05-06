package dev.digitaldragon;

import static spark.Spark.get;
import static spark.Spark.post;

import dev.digitaldragon.database.mongo.MongoManager;
import dev.digitaldragon.queue.CrawlManager;
import dev.digitaldragon.queue.ItemManager;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class Main {
    public static void main(String[] args) {
        MongoManager.initializeDb();

        get("/queue", (request, response) ->  {
            response.type("application/json");

            String username;
            try {
                username = validateUsername(request.queryParams("username"));
            } catch (IllegalArgumentException e) {
                JSONObject responseJson = new JSONObject();
                responseJson.put("error", "must use a username");
                return responseJson.toString();
            }

            int amount = 1;
            try {
                amount = Integer.parseInt(request.queryParams("amount"));
                if (amount <= 0) {
                    amount = 1;
                }
                if (amount > 2000) {
                    amount = 2000;
                }
            } catch (NumberFormatException e) {
                // Ignore exception and use default amount of 1
            }

            JSONArray urlsJsonArray = new JSONArray();
            List<String> urls = CrawlManager.getUniqueDomainUrls(amount, username);

            for (String s : urls) {
                urlsJsonArray.put(s);
                System.out.println(s);
            }

            JSONObject responseJson = new JSONObject();
            responseJson.put("urls", urlsJsonArray);

            return responseJson.toString();
        });

        post("/queue", (request, response) -> {
            response.type("application/json");

            String username;
            try {
                username = validateUsername(request.queryParams("username"));
            } catch (IllegalArgumentException e) {
                JSONObject responseJson = new JSONObject();
                responseJson.put("error", "must use a username");
                return responseJson.toString();
            }

            JSONObject jsonObject = new JSONObject(request.body());
            JSONArray urls = jsonObject.getJSONArray("urls");
            Set<String> urlSet = new HashSet<>();

            for (int i = 0; i < urls.length(); i++) {
                urlSet.add(urls.getString(i));
            }
            ItemManager.bulkQueueURLs(urlSet, username);

            return new JSONObject().put("success", "true").toString();
        });


        post("/submit", (request, response) ->  {
            response.type("application/json");

            String username;
            try {
                username = validateUsername(request.queryParams("username"));
            } catch (IllegalArgumentException e) {
                JSONObject responseJson = new JSONObject();
                responseJson.put("error", "must use a username");
                return responseJson.toString();
            }



            System.out.println(request.body());  //todo logging print

            //grab and parse json
            JSONObject jsonObject = new JSONObject(request.body());
            String crawlerUsername = jsonObject.getString("username");
            System.out.println(crawlerUsername); //todo logging print

            //queue up discovered urls
            JSONArray discovered = jsonObject.getJSONArray("discovered");
            System.out.println("Deduplicating and queueing " + discovered.length() + " urls.");
            for (int i = 0; i < discovered.length(); i++ ) {
                ItemManager.queueURL(discovered.get(i).toString(), username);
            }

            //submit finished url to done
            ItemManager.submitCrawlInfo(jsonObject);

            return "{\"status\":\"ok\"}";
        });
    }
    private static String validateUsername(String username) {
        if (username == null || username.isEmpty()) {
            throw new IllegalArgumentException("Username is required");
        }
        return username;
    }
}