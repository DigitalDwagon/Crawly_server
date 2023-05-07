package dev.digitaldragon.database;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.*;
import dev.digitaldragon.database.mongo.MongoManager;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WriteManager {
    private static List<WriteModel<Document>> queueWrites = new ArrayList<>();
    private static List<WriteModel<Document>> outWrites = new ArrayList<>();
    private static List<WriteModel<Document>> doneWrites = new ArrayList<>();
    private static List<WriteModel<Document>> duplicatesWrites = new ArrayList<>();
    private static List<WriteModel<Document>> rejectsWrites = new ArrayList<>();

    public static void itemAdd(Database database, JSONObject jsonObject) {
        Document document = Document.parse(jsonObject.toString());
        switch (database) {
            case QUEUE -> queueWrites.add(new InsertOneModel<>(document));
            case OUT -> outWrites.add(new InsertOneModel<>(document));
            case DONE -> doneWrites.add(new InsertOneModel<>(document));
            case DUPLICATES -> duplicatesWrites.add(new InsertOneModel<>(document));
            case REJECTS -> rejectsWrites.add(new InsertOneModel<>(document));
        }
        flush();
    }

    public static void itemRemove(Database database, String url) {
        Bson filter = Filters.eq("url", url);
        switch (database) {
            case QUEUE -> queueWrites.add(new DeleteOneModel<>(filter));
            case OUT -> outWrites.add(new DeleteOneModel<>(filter));
            case DONE -> doneWrites.add(new DeleteOneModel<>(filter));
            case DUPLICATES -> duplicatesWrites.add(new DeleteOneModel<>(filter));
            case REJECTS -> rejectsWrites.add(new DeleteOneModel<>(filter));
        }
        flush();
    }

    @Deprecated //TODO TEMPORARY METHOD WHILE SWITCHING EVERYTHING OVER
    public static void proxyBulkWrites(Database database, List<WriteModel<Document>> writes) {
        switch (database) {
            case QUEUE -> queueWrites.addAll(writes);
            case OUT -> outWrites.addAll(writes);
            case DONE -> doneWrites.addAll(writes);
            case DUPLICATES -> duplicatesWrites.addAll(writes);
            case REJECTS -> rejectsWrites.addAll(writes);
        }
        flush();
    }
    public static void flush(boolean force) {
        MongoCollection<Document> queueCollection = MongoManager.getQueueCollection();
        MongoCollection<Document> outCollection = MongoManager.getOutCollection();
        MongoCollection<Document> doneCollection = MongoManager.getDoneCollection();
        MongoCollection<Document> duplicatesCollection = MongoManager.getDuplicatesCollection();
        MongoCollection<Document> rejectsCollection = MongoManager.getRejectsCollection();

        List<List<WriteModel<Document>>> writeLists = Arrays.asList(
                queueWrites, outWrites, doneWrites, duplicatesWrites, rejectsWrites);
        List<MongoCollection<Document>> collectionList = Arrays.asList(
                queueCollection, outCollection, doneCollection, duplicatesCollection, rejectsCollection);

        for (int i = 0; i < writeLists.size(); i++) {
            List<WriteModel<Document>> writes = writeLists.get(i);
            MongoCollection<Document> collection = collectionList.get(i);
            if ((force || writes.size() >= 5000) && !writes.isEmpty()) {
                collection.bulkWrite(writes);
                writes.clear();
            }
        }
    }

    public static void flush() {
        flush(false);
    }
}
