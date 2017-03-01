package com.msse.bigdata;

import com.mongodb.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.xml.sax.SAXException;
import javax.xml.parsers.ParserConfigurationException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


@SpringBootApplication
public class IrsformsApplication {

	public static void main(String[] args) throws IOException, ParserConfigurationException, SAXException {
		SpringApplication.run(IrsformsApplication.class, args);

		Mongo mongo = new Mongo("localhost", 27017);
		DB db = mongo.getDB("mydb");

		DBCollection collection = db.getCollection("mycollection");
		BasicDBObject document;
		JSONParser parser = new JSONParser();

		List<String> urls = new ArrayList<>();

		Integer counter = 0;

		try {

			parser = new JSONParser();
			Object obj = parser.parse(new FileReader("/2015.json"));
			JSONObject jsonObject = (JSONObject) obj;
			JSONArray filings = (JSONArray) jsonObject.get("Filings2015");

			for(Object filing : filings) {
				document = new BasicDBObject();
				JSONObject jsonNumber = (JSONObject) filing;
				String ein = (String) jsonNumber.get("EIN");
				String url = (String) jsonNumber.get("URL");
				urls.add(url);
				document.put(ein, jsonNumber.toString());
				collection.insert(document);
				counter++;
			}
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println(counter);
		System.out.println(urls.size());

		for(String urlLink: urls) {



		}





	}
}
