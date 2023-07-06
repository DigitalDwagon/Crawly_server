package dev.digitaldragon.database;

import com.mongodb.client.MongoCollection;
import dev.digitaldragon.database.mongo.MongoManager;
import org.bson.Document;

public enum Database {
    QUEUE, OUT, DONE, DUPLICATES, REJECTS, PROCESSING, BIGQUEUE;
    
    public static MongoCollection<Document> toCollection(Database database) {
        MongoCollection<Document> collection = null;
        switch (database) {
            case OUT -> collection = MongoManager.getOutCollection();
            case QUEUE -> collection = MongoManager.getQueueCollection();
            case DONE -> collection = MongoManager.getDoneCollection();
            case DUPLICATES -> collection = MongoManager.getDuplicatesCollection();
            case REJECTS -> collection = MongoManager.getRejectsCollection();
            case PROCESSING -> collection = MongoManager.getProcessingCollection();
            case BIGQUEUE -> collection = MongoManager.getBigqueueCollection();
        }
        return collection;
    }
}
