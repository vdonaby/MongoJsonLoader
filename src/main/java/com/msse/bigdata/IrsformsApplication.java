package com.msse.bigdata;

import com.mongodb.*;
import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.xml.sax.SAXException;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
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
		List<Object> indexes = new ArrayList<>();

		Integer numberOfIndexes = 0;
		Integer numberOfFilings = 0;



		try {

			File path = new File("/Users/z001hk8/Desktop/UOFM/2017/BigData/IRSData/Indexes/");
			File [] files = path.listFiles();
			for (int i = 11; i <= files.length; i++){
				if (files[i].isFile()){ //this line weeds out other directories/folders
					parser = new JSONParser();
					Object indexYear = parser.parse(new FileReader(files[i]));
					JSONObject jsonObject = (JSONObject) indexYear;
					JSONArray indexYearJSON = (JSONArray) jsonObject.get("Filings20" + i);
					indexes.add(indexYearJSON);
				}
			}

			for(Object index : indexes) {
				document = new BasicDBObject();
				JSONObject jsonNumber = (JSONObject) index;
				String ein = (String) jsonNumber.get("EIN");
				String url = (String) jsonNumber.get("URL");
				urls.add(url);
				document.put(ein, jsonNumber.toString());
				collection.insert(document);
				numberOfIndexes++;
			}

		} catch (ParseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		for(String urlLink: urls) {
			StringBuilder result = new StringBuilder();
			URL url = new URL(urlLink);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			String line;
			while ((line = rd.readLine()) != null) {
				result.append(line);
			}
			rd.close();
			filingDocuments.add(result.toString());
		}

		//TODO Convert XML document to JSON

		for(Object filingDocument : filingDocuments) {
			document = new BasicDBObject();
			JSONObject jsonNumber = (JSONObject) filingDocument;
			String ein = (String) jsonNumber.get("EIN");
			document.put(ein, jsonNumber.toString());
			collection.insert(document);
			numberOfFilings++;
		}

	}
}
