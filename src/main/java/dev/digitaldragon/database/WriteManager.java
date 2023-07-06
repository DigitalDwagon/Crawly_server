package dev.digitaldragon.database;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.*;
import dev.digitaldragon.database.mongo.MongoManager;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.JSONObject;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WriteManager {
    private static List<WriteModel<Document>> processingWrites = new ArrayList<>();
    private static List<WriteModel<Document>> bigqueueWrites = new ArrayList<>();
    private static List<WriteModel<Document>> queueWrites = new ArrayList<>();
    private static List<WriteModel<Document>> outWrites = new ArrayList<>();
    private static List<WriteModel<Document>> doneWrites = new ArrayList<>();
    private static List<WriteModel<Document>> duplicatesWrites = new ArrayList<>();
    private static List<WriteModel<Document>> rejectsWrites = new ArrayList<>();

    public static void itemAdd(Database database, JSONObject jsonObject) {
        Document document = Document.parse(jsonObject.toString());
        itemAdd(database, document);
    }

    public static void itemAdd(Database database, Document document) {
        document.remove("_id");
        switch (database) {
            case PROCESSING -> processingWrites.add(new InsertOneModel<>(document));
            case BIGQUEUE -> bigqueueWrites.add(new InsertOneModel<>(document));
            case QUEUE -> queueWrites.add(new InsertOneModel<>(document));
            case OUT -> outWrites.add(new InsertOneModel<>(document));
            case DONE -> MongoManager.getDoneCollection().insertOne(document);
            case DUPLICATES -> duplicatesWrites.add(new InsertOneModel<>(document));
            case REJECTS -> rejectsWrites.add(new InsertOneModel<>(document));
        }
        flush();
    }

    public static void itemRemove(Database database, String url) {
        Bson filter = Filters.eq("url", url);
        switch (database) {
            case PROCESSING -> processingWrites.add(new DeleteOneModel<>(filter));
            case BIGQUEUE -> bigqueueWrites.add(new DeleteOneModel<>(filter));
            case QUEUE -> queueWrites.add(new DeleteOneModel<>(filter));
            case OUT -> outWrites.add(new DeleteOneModel<>(filter));
            case DONE -> doneWrites.add(new DeleteOneModel<>(filter));
            case DUPLICATES -> duplicatesWrites.add(new DeleteOneModel<>(filter));
            case REJECTS -> rejectsWrites.add(new DeleteOneModel<>(filter));
        }
        flush();
    }

    public static void submitFinishedItem(Document document) {
        MongoCollection<Document> outCollection = MongoManager.getOutCollection();
        MongoCollection<Document> doneCollection = MongoManager.getDoneCollection();

        Bson filter = Filters.eq("url", document.get("url"));
        processingWrites.add(new DeleteManyModel<>(filter));
        bigqueueWrites.add(new DeleteManyModel<>(filter));
        queueWrites.add(new DeleteManyModel<>(filter));
        Document outDocument = outCollection.findOneAndDelete(filter);
        if (outDocument != null) {
            for (String key : outDocument.keySet()) {
                if (!document.containsKey(key)) { //Trust our submitted data over the out document for collisions (ie url)
                    document.put(key, outDocument.get(key));
                }
            }
        }
        doneCollection.insertOne(document);
    }

    public static void moveDocument(String url, Database start, Database end) {
        MongoCollection<Document> startCollection = Database.toCollection(start);
        MongoCollection<Document> endCollection = Database.toCollection(end);
        Bson filter = Filters.eq("url", url);

        Document document = startCollection.findOneAndDelete(filter);
        if (document != null) {
            endCollection.insertOne(document);
        }
    }

    public static void checkoutQueueItem(String username, String url) {
        MongoCollection<Document> startCollection = MongoManager.getQueueCollection();
        MongoCollection<Document> endCollection = MongoManager.getOutCollection();
        Bson filter = Filters.eq("url", url);

        Document document = startCollection.findOneAndDelete(filter);
        if (document != null) {
            document.append("out_for", username);
            document.append("out_at", Instant.now());
            endCollection.insertOne(document);
        }
    }

    @Deprecated //TODO TEMPORARY METHOD WHILE SWITCHING EVERYTHING OVER
    public static void proxyBulkWrites(Database database, List<WriteModel<Document>> writes) {
        switch (database) {
            case PROCESSING -> processingWrites.addAll(writes);
            case BIGQUEUE -> bigqueueWrites.addAll(writes);
            case QUEUE -> queueWrites.addAll(writes);
            case OUT -> outWrites.addAll(writes);
            case DONE -> doneWrites.addAll(writes);
            case DUPLICATES -> duplicatesWrites.addAll(writes);
            case REJECTS -> rejectsWrites.addAll(writes);
        }
        flush();
    }
    public static void flush(boolean force) {
        MongoCollection<Document> processingCollection = MongoManager.getProcessingCollection();
        MongoCollection<Document> bigqueueCollection = MongoManager.getBigqueueCollection();
        MongoCollection<Document> queueCollection = MongoManager.getQueueCollection();
        MongoCollection<Document> outCollection = MongoManager.getOutCollection();
        MongoCollection<Document> doneCollection = MongoManager.getDoneCollection();
        MongoCollection<Document> duplicatesCollection = MongoManager.getDuplicatesCollection();
        MongoCollection<Document> rejectsCollection = MongoManager.getRejectsCollection();

        List<List<WriteModel<Document>>> writeLists = Arrays.asList(
                processingWrites, bigqueueWrites, queueWrites, outWrites, doneWrites, duplicatesWrites, rejectsWrites);
        List<MongoCollection<Document>> collectionList = Arrays.asList(
                processingCollection, bigqueueCollection, queueCollection, outCollection, doneCollection, duplicatesCollection, rejectsCollection);

        for (int i = 0; i < writeLists.size(); i++) {
            List<WriteModel<Document>> writes = writeLists.get(i);
            MongoCollection<Document> collection = collectionList.get(i);
            if ((force || writes.size() >= 1000) && !writes.isEmpty()) {
                collection.bulkWrite(writes, new BulkWriteOptions().ordered(false));
                LoggerFactory.getLogger(WriteManager.class).info(String.format("Flushed %s writes to %s", writes.size(), collection.getNamespace()));
                writes.clear();
            }
        }
    }

    public static void flush() {
        flush(false);
    }
}
