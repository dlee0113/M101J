package com.test;

import java.util.ArrayList;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class MongoDBJDBC {
	public static void main(String args[]) {
		try {
			// To connect to mongodb server
			MongoClient mongoClient = new MongoClient("localhost", 27017);

			// Now connect to your databases
			DB db = mongoClient.getDB("students");
			System.out.println("Connect to database successfully");
//			boolean auth = db.authenticate("", "".toCharArray());
//			System.out.println("Authentication: " + auth);
			
			DBCollection coll = db.getCollection("grades");
	        System.out.println("Collection mycol selected successfully");
	        
//			$group : {
//				_id : "$student_id", 
//				min_score : {$min : "$score"},
//				objectId : {$first : "$_id"}
//			}
	        
//	        DBObject groupFields = new BasicDBObject("_id", "$student_id").append("min_score", new BasicDBObject( "$min", "$score"));
	        DBObject groupFields = new BasicDBObject("_id", "$student_id").
		        							append("min_score", new BasicDBObject("$min", "$score")).
		        							append("objectId", new BasicDBObject("$first", "$_id"));
	        
	        Iterable<DBObject> output = coll.aggregate(
	        		(DBObject) new BasicDBObject("$match", new BasicDBObject("type", "homework")),
	        		(DBObject) new BasicDBObject("$group", groupFields),
	        		(DBObject) new BasicDBObject("$sort",  new BasicDBObject("_id", 1))
	        ).results();
	        
	        BasicDBObject query = new BasicDBObject();
	        List<Integer> list = new ArrayList<Integer>();
	        
//	        collection.remove(new BasicDBObject().append("empId", 10002));
	        
	        for (DBObject dbObject : output)
	        {
	            System.out.println(dbObject);
	            list.add((Integer) dbObject.get("_id"));
	            
	        }
	        System.out.println(list);
	        
		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}
	}
}