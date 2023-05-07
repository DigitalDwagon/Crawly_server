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

        if ((queueWrites.size() >= 5000 || force) && !queueWrites.isEmpty()) {
            queueCollection.bulkWrite(queueWrites);
            queueWrites.clear();
        }
        if ((outWrites.size() >= 5000 || force) && !outWrites.isEmpty()) {
            outCollection.bulkWrite(outWrites);
            outWrites.clear();
        }
        if ((doneWrites.size() >= 5000 || force) && !doneWrites.isEmpty()) {
            doneCollection.bulkWrite(doneWrites);
            doneWrites.clear();
        }
        if ((duplicatesWrites.size() >= 5000 || force) && !duplicatesWrites.isEmpty()) {
            duplicatesCollection.bulkWrite(duplicatesWrites);
            duplicatesWrites.clear();
        }
        if ((rejectsWrites.size() >= 5000 || force) && !rejectsWrites.isEmpty()) {
            rejectsCollection.bulkWrite(rejectsWrites);
            rejectsWrites.clear();
        }
    }

    public void flush() {
        flush(false);
    }
}
