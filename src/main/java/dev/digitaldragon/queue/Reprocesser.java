package dev.digitaldragon.queue;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;

public class Reprocesser {
    public static void processCollectionDocuments(MongoCollection<Document> sourceCollection, MongoCollection<Document> targetCollection) {
        try (MongoCursor<Document> cursor = sourceCollection.find().iterator()){
            while (cursor.hasNext()) {
                Document document = cursor.next();
                sourceCollection.findOneAndDelete(document);

                // Removing the fields
                document.remove("processed_at");
                document.remove("status");
                document.remove("_id");

                // Adding the document to the processing collection
                targetCollection.insertOne(document);
            }
        }
    }
}
