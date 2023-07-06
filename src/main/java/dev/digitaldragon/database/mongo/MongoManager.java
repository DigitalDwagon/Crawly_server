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
    @Getter
    private static MongoCollection<Document> bigqueueCollection;
    @Getter
    private static MongoCollection<Document> processingCollection;
    @Getter
    private static MongoCollection<Document> dnsCacheCollection;
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
        processingCollection = database.getCollection("processing");
        bigqueueCollection = database.getCollection("bigqueue");
        queueCollection = database.getCollection("queue");
        rejectsCollection = database.getCollection("rejects");
        duplicatesCollection = database.getCollection("duplicates");
        dnsCacheCollection = database.getCollection("dnscache");

    }

}
