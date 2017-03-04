package com.msse.bigdata;

import com.mongodb.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.client.RestTemplate;
import org.xml.sax.SAXException;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
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
		List<String> filingDocuments = new ArrayList<>();
		List<JSONArray> indexes = new ArrayList<>();

		Integer numberOfIndexes = 0;
		Integer numberOfFilings = 0;

		int year = 2011;

		try {

			File path = new File("/Users/z001hk8/Desktop/UOFM/2017/BigData/IRSData/Indexes/");
			File [] files = path.listFiles();
			for (int i = 1; i < files.length; i++){
				if (files[i].isFile()){ //this line weeds out other directories/folders
					parser = new JSONParser();
					System.out.println("Files: " + files[i]);
					Object indexYear = parser.parse(new FileReader(files[i]));
					JSONObject jsonObject = (JSONObject) indexYear;
					JSONArray indexYearJSON = (JSONArray)jsonObject.get("Filings" + year);
					indexes.add(indexYearJSON);
					year++;
				}
			}

			for(JSONArray index : indexes) {

				for (int i = 0 ; i < index.size(); i++) {
					JSONObject jsonObj = (JSONObject) index.get(i);
					String ein = jsonObj.get("EIN").toString();
					String url = jsonObj.get("URL").toString();
					urls.add(url);
					document = new BasicDBObject();
					document.put(ein, jsonObj.toString());
					collection.insert(document);
					numberOfIndexes++;
				}

			}
			System.out.println("number of indexes: " + numberOfIndexes);

		} catch (ParseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}


		RestTemplate restTemplate = new RestTemplate();

		for(String urlLink: urls) {
			filingDocuments.add(restTemplate.getForObject(urlLink, String.class));
		}

		int filingDocNumber = 0;

		for(String filingDocument : filingDocuments) {
			document = new BasicDBObject();
			document.put(String.valueOf(filingDocNumber), filingDocument);
			collection.insert(document);
			numberOfFilings++;
		}

		System.out.println("number of filings: " + numberOfFilings);

	}
}
