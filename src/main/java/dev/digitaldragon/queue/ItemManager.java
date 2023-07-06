package dev.digitaldragon.queue;

import dev.digitaldragon.database.Database;
import dev.digitaldragon.database.ReadManager;
import dev.digitaldragon.database.WriteManager;
import org.bson.Document;
import org.json.JSONObject;

import javax.print.Doc;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Time;
import java.time.DateTimeException;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ItemManager {
    public static void bulkQueueURLs(Set<String> urls, String username, String reason) {
        System.out.println(urls);
        for (String url : urls) {
            System.out.println("Processing: " + url);
            JSONObject write = new JSONObject().put("url", url)
                    .put("queued_at", Time.from(Instant.now()).toString())
                    .put("queued_by", username)
                    .put("queue_reason", reason);


            try { // Quick prefiltering
                URI uri = new URI(url);
                if (uri.getScheme() == null || uri.getHost() == null) {
                    throw new URISyntaxException(url, "bad host/scheme");
                }
            } catch (URISyntaxException exception) {
                write.put("processed_at", Instant.now());
                write.put("status", "REJECTED");
                write.put("reject_reason", "INVALID_URL_PREFILTERED");
                //todo logging print
                System.out.println("pre-rejected:");
                System.out.println(write.toString());

                WriteManager.itemAdd(Database.REJECTS, write);
            }
            System.out.println("to be processed:");
            System.out.println(write.toString());
            WriteManager.itemAdd(Database.PROCESSING, write);
        }
    };

    public static void submitCrawlInfo(JSONObject submittedData) {
        // TODO validate provided data
        String url = submittedData.getString("url");
        String username = submittedData.getString("username");

        //data validation
        Document verified = new Document();
        verified.put("submitted_at", Instant.now());
        verified.put("submitted_by", username);
        verified.put("url", submittedData.getString("url"));

        Document crawl = new Document();
        crawl.put("discovered_embeds", submittedData.getJSONArray("discovered_embeds"));
        crawl.put("discovered_outlinks", submittedData.getJSONArray("discovered_outlinks"));
        crawl.put("user_agent", submittedData.getString("user_agent"));
        crawl.put("response_code", submittedData.get("response"));
        if (submittedData.has("ip"))
            crawl.put("reported_ip", submittedData.get("ip"));
        if (submittedData.has("time")) {
            Instant parsedInstant = Instant.parse(submittedData.getString("time"));
            Instant currentInstant = Instant.now();
            Instant oneHourAgo = currentInstant.minusSeconds(3600); // Subtract one hour (3600 seconds)

            if (!parsedInstant.isAfter(oneHourAgo)) {
                throw new DateTimeException("More than an hour ago!");
            }
            crawl.put("time", submittedData.getString("time"));

        }
        crawl.put("client", submittedData.getString("client"));

        verified.put("crawl", crawl);
        WriteManager.submitFinishedItem(verified);
    }
}
