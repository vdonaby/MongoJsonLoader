package com.msse.bigdata;

import com.mongodb.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;


@SpringBootApplication
public class IrsformsApplication {

	public static void main(String[] args) throws FileNotFoundException {
		SpringApplication.run(IrsformsApplication.class, args);

		Mongo mongo = new Mongo("localhost", 27017);
		DB db = mongo.getDB("mydb");

		DBCollection collection = db.getCollection("mycollection");
		BasicDBObject document;
		JSONParser parser = new JSONParser();

		Integer counter = 0;

		try {

			parser = new JSONParser();
			Object obj = parser.parse(new FileReader("/2015.json"));
			JSONObject jsonObject = (JSONObject) obj;
			JSONArray numbers = (JSONArray) jsonObject.get("Filings2015");

			for (Object number : numbers) {
				document = new BasicDBObject();
				JSONObject jsonNumber = (JSONObject) number;
				String natural = (String) jsonNumber.get("EIN");
				document.put(natural, jsonNumber.toString());
				collection.insert(document);
				counter++;

			}
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println(counter);

	}
}
