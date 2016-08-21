package com.test;

import static com.mongodb.client.model.Filters.eq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class Homework31 {
	public static void main(String args[]) {
		try {
			// To connect to mongodb server
			MongoClient mongoClient = new MongoClient("localhost", 27017);

			// Now connect to your databases
			MongoDatabase database = mongoClient.getDatabase("school");
			System.out.println("Connect to database successfully");
//			boolean auth = db.authenticate("", "".toCharArray());
//			System.out.println("Authentication: " + auth);
			
			MongoCollection<Document> collection = database.getCollection("students");
	        System.out.println("Collection mycol selected successfully");
	        
	        System.out.println(collection.count());
	        
//	        -- find documents with minimum homework score
//	        db.students.aggregate( [
//	          { '$unwind': '$scores' },
//	          { $match : { 'scores.type' : "homework" } } ,
//	          {
//	            '$group':
//	            {
//	              '_id': '$_id',
//	              'min': { $min: '$scores.score' }
//	            }
//	          }
//	        ] )
	        
	        AggregateIterable<Document> iterable = collection.aggregate(Arrays.asList(
	        	new Document("$unwind", "$scores"),
	    		new Document("$match", new Document("scores.type", "homework")),
	    		new Document("$group", new Document("_id", "$_id").append("min", new Document("$min", "$scores.score"))),
	        	new Document("$sort", new Document("_id", 1)))
	        );
	        
	        List<Document> documents = new ArrayList<Document>();
	        
	        Iterator<Document> iterator = iterable.iterator();
	        while (iterator.hasNext()) {
	        	Document document = iterator.next();
	        	System.out.println(document);
	        	
	        	documents.add(document);
	        }

//	        -- remove found documents
//	        db.students.update(
//        		{ _id: 180 },
//        		{ 
//        			$pull: { 
//        				'scores': { 
//        					score: 74.41836270655918, type: 'homework' } 
//        				} 
//        		}
//        	);
 
	        for (Document document : documents) {
	        	System.out.println(document);
	        	Integer id = (Integer) document.get("_id");
	        	Double min = (Double) document.get("min");
	        	
	        	collection.updateOne(
	        		new Document("_id", id),
	        		new Document("$pull", 
    	        		new Document("scores", 
    		        		new Document("score", min).append("type", "homework")
    	        		)
    	        	)
	        	);
	        }
	        
	        System.out.println(collection.count());
		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}
	}
}