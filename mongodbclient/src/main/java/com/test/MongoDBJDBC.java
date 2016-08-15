package com.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.gte;

public class MongoDBJDBC {
	public static void main(String args[]) {
		try {
			// To connect to mongodb server
			MongoClient mongoClient = new MongoClient("localhost", 27017);

			// Now connect to your databases
			MongoDatabase database = mongoClient.getDatabase("students");
			System.out.println("Connect to database successfully");
//			boolean auth = db.authenticate("", "".toCharArray());
//			System.out.println("Authentication: " + auth);
			
			MongoCollection<Document> collection = database.getCollection("grades");
	        System.out.println("Collection mycol selected successfully");
	        
//	        Document first = collection.find().first();
//	        System.out.println(first);
	        
	        AggregateIterable<Document> iterable = collection.aggregate(Arrays.asList(
	        		new Document("$match", new Document("type", "homework")),
	        		new Document("$sort", new Document("student_id", 1).append("score", 1)))
	        );
	        
	        int count = 0;
	        List<Document> documents = new ArrayList<Document>();
	        
	        Iterator<Document> iterator = iterable.iterator();
	        while (iterator.hasNext()) {
	        	Document document = iterator.next();
	        	
	        	if (count % 2 == 0) {
	        		documents.add(document);
	        	}
	        	
	        	count++;
	        }
	        
	        System.out.println();
	        System.out.println(documents.size());
	        
	        for (Document document : documents) {
	        	System.out.println(document);
	        	collection.deleteOne(eq("_id", document.getObjectId("_id")));
	        }
	        
	        System.out.println(collection.count());
		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}
	}
}