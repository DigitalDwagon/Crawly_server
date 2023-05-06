package dev.digitaldragon.database.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.Getter;
import org.bson.Document;

public class MongoManager {

    @Getter
    private static MongoManager instance;
    @Getter
    private MongoManager mongoManager;
    @Getter
    private static MongoDatabase database;
    @Getter
    private static MongoCollection<Document> doneCollection;
    @Getter
    private static MongoCollection<Document> outCollection;
    @Getter
    private static MongoCollection<Document> rejectsCollection;
    @Getter
    private static MongoCollection<Document> duplicatesCollection;
    @Getter
    private static MongoCollection<Document> queueCollection;
    private static final String username = "nessiebot",
            password = "tRMtsVUgLbv2asyRYcQBH3fa76TSwNrUT373Hb5tA9FuwGrjWNp9cbkstpTTsU6xPZdsbHVmGfVmmzGG9CxbhxS3nUCAAhGGXghJpQdDUKgUvhKM6mVWN2Qph9mwQs72",
            host = "localhost",
            port = "27017";
    @Getter
    private static MongoClient mongoClient;
    public MongoManager() {
        instance = this;

    }
    public static void initializeDb() {
        mongoClient = MongoClients.create("mongodb://localhost:27017");
        database = mongoClient.getDatabase("crawly");
        doneCollection = database.getCollection("done");
        outCollection = database.getCollection("out");
        queueCollection = database.getCollection("queue");
        rejectsCollection = database.getCollection("rejects");
        duplicatesCollection = database.getCollection("duplicates");
    }

}
