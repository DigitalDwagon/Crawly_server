package dev.digitaldragon.database.mongo;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;

import javax.print.Doc;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class UrlsExporter {
    public static void export(List<MongoCollection<Document>> collections) {
        try (FileWriter writer = new FileWriter("export.txt")) {
            for (MongoCollection<Document> collection : collections) {
                // Find all documents in the collection
                MongoCursor<Document> cursor = collection.find().iterator();
                while (cursor.hasNext()) {
                    Document document = cursor.next();

                    // Extract the "url" field from the document
                    String url = document.getString("url");

                    // Write the URL to the output file
                    writer.write(url + "\n");
                }

                cursor.close();
            }
            System.out.println("Export completed successfully.");
        } catch (IOException e) {
            System.err.println("Error writing to file: " + e.getMessage());
        }
    }
}
