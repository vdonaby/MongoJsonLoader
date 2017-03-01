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


//			Object filings2011 = parser.parse(new FileReader("/Users/z001hk8/Desktop/UOFM/2017/BigData/IRSData/2011.json"));
//			Object filings2012 = parser.parse(new FileReader("/Users/z001hk8/Desktop/UOFM/2017/BigData/IRSData/2012.json"));
//			Object filings2013 = parser.parse(new FileReader("/Users/z001hk8/Desktop/UOFM/2017/BigData/IRSData/2013.json"));
//			Object filings2014 = parser.parse(new FileReader("/Users/z001hk8/Desktop/UOFM/2017/BigData/IRSData/2014.json"));
//			Object filings2015 = parser.parse(new FileReader("/Users/z001hk8/Desktop/UOFM/2017/BigData/IRSData/2014.json"));
//			Object filings2016 = parser.parse(new FileReader("/Users/z001hk8/Desktop/UOFM/2017/BigData/IRSData/2016.json"));
//
//			JSONObject jsonObject2011 = (JSONObject) filings2011;
//			JSONObject jsonObject2012 = (JSONObject) filings2012;
//			JSONObject jsonObject2013 = (JSONObject) filings2013;
//			JSONObject jsonObject2014 = (JSONObject) filings2014;
//			JSONObject jsonObject2015 = (JSONObject) filings2015;
//			JSONObject jsonObject2016 = (JSONObject) filings2016;
//
//			JSONArray indexes2011 = (JSONArray) jsonObject2011.get("Filings2011");
//			JSONArray indexes2012 = (JSONArray) jsonObject2012.get("Filings2012");
//			JSONArray indexes2013 = (JSONArray) jsonObject2013.get("Filings2013");
//			JSONArray indexes2014 = (JSONArray) jsonObject2014.get("Filings2014");
//			JSONArray indexes2015 = (JSONArray) jsonObject2015.get("Filings2014");
//			JSONArray indexes2016 = (JSONArray) jsonObject2016.get("Filings2016");
//
//			indexes.add(indexes2011);
//			indexes.add(indexes2012);
//			indexes.add(indexes2013);
//			indexes.add(indexes2014);
//			indexes.add(indexes2015);
//			indexes.add(indexes2016);

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
