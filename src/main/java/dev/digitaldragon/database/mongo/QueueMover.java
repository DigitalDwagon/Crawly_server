package dev.digitaldragon.database.mongo;

import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;

public class QueueMover {
    public static void move(int amount) {
        int done = 0;
        MongoCollection<Document> queueCollection = MongoManager.getQueueCollection();
        MongoCollection<Document> bigQueueCollection = MongoManager.getBigqueueCollection();

        try (MongoCursor<Document> cursor = bigQueueCollection.find().iterator()) {
            while (cursor.hasNext() && done < amount) {
                Document document = cursor.next();
                try {
                    queueCollection.insertOne(document);
                    System.out.println("Moved " + document.get("url") + " to queue.");
                    bigQueueCollection.deleteOne(document);
                } catch (MongoWriteException e) {
                    if (e.getCode() == 11000) {
                        System.out.println("Skipping duplicate.");
                    } else {
                        e.printStackTrace();
                    }
                }

                done++;
            }
        }
    }
}
