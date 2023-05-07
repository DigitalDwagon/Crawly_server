package dev.digitaldragon.database;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import dev.digitaldragon.database.mongo.MongoManager;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WriteManager {
    private static List<WriteModel<Document>> queueWrites = new ArrayList<>();
    private static List<WriteModel<Document>> outWrites = new ArrayList<>();
    private static List<WriteModel<Document>> doneWrites = new ArrayList<>();
    private static List<WriteModel<Document>> duplicatesWrites = new ArrayList<>();
    private static List<WriteModel<Document>> rejectsWrites = new ArrayList<>();

    public void itemAdd(Database database, JsonObject jsonObject) {
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
