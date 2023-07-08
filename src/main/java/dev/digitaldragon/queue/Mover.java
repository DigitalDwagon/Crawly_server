package dev.digitaldragon.queue;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import dev.digitaldragon.database.Database;
import dev.digitaldragon.database.ReadManager;
import dev.digitaldragon.database.WriteManager;
import dev.digitaldragon.database.mongo.MongoManager;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.JSONObject;

import java.util.List;

public class Mover {
    public static void move() {
        MongoCollection<Document> bigQueueCollection = MongoManager.getBigqueueCollection();
        MongoCollection<Document> queueCollection = MongoManager.getQueueCollection();
        int retries = 0;

        while (true) {
            if (queueCollection.countDocuments() > 300000 || bigQueueCollection.countDocuments() <= 0) {
                continue;
            }

            try (MongoCursor<Document> cursor = bigQueueCollection.aggregate(
                    List.of(new Document("$sample", new Document("size", 2000)))).iterator()) {
                while (cursor.hasNext()) {
                    Document document = cursor.next();
                    String url = document.get("url").toString();
                    System.out.println("moving: " + url); //todo logging print
                    Bson filter = Filters.eq("url", url);
                    WriteManager.itemRemove(Database.BIGQUEUE, url);
                    WriteManager.itemAdd(Database.QUEUE, document);
                }


            } catch (MongoException exception) {
                exception.printStackTrace();
            }
            retries++;
        }
    }
}
