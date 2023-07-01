package dev.digitaldragon.queue;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import dev.digitaldragon.database.Database;
import dev.digitaldragon.database.ReadManager;
import dev.digitaldragon.database.WriteManager;
import dev.digitaldragon.database.mongo.MongoManager;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.JSONObject;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Time;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.mongodb.client.model.Filters.eq;

public class ItemManager {
    public static void bulkQueueURLs(Set<String> urls, String username) { //TODO read cache
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            for (String url : urls) {
                // Quick filtering for absolute garbage
                Database endDatabase = Database.REJECTS;
                try {
                    URI uri = new URI(url);
                    if (uri.getScheme() == null || uri.getHost() == null) {
                        continue;
                    }
                    endDatabase = Database.PROCESSING;
                } catch (URISyntaxException e) {
                    //do nothing, already assigned as a reject
                }

                // Quick duplication check
                if (ReadManager.cacheCheckDuplication(url)) {
                    endDatabase = Database.DUPLICATES;
                }

                JSONObject write = new JSONObject()
                        .put("url", url)
                        .put("queuedAt", Time.from(Instant.now()).toString())
                        .put("queuedBy", username);

                WriteManager.itemAdd(endDatabase, write);
            }
        });
    }

    public static void submitCrawlInfo(JSONObject data) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            data.put("submittedAt", Instant.now());

            // TODO validate provided data
            String url = data.get("url").toString();
            data.remove("_id");
            WriteManager.itemAdd(Database.DONE, data);
            WriteManager.itemRemove(Database.QUEUE, url);
            WriteManager.itemRemove(Database.OUT, url);

        });
    }
}
