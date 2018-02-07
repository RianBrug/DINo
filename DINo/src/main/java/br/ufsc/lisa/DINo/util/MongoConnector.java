package br.ufsc.lisa.DINo.util;

import java.util.Arrays;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;

public class MongoConnector implements Connector{
	
	private MongoClient client;
	private MongoCredential credential;
	private MongoDatabase db;
	
	public boolean put(String key, String json) {
		Document doc = Document.parse(json);
		doc.replace("_id", key);
		this.db.getCollection("teste").insertOne(doc);
		return true;
	}
	@Override
	public boolean connect(String uri, String port, String user, String password, String DB) {
		if (user != null && !user.isEmpty()) {
			try {
				this.credential = MongoCredential.createCredential(user, DB, password.toCharArray());
				this.client = new MongoClient(new ServerAddress(uri, Integer.valueOf(port)), Arrays.asList(credential));
				db = this.client.getDatabase(DB);	
			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}
		} else {
			try {
				this.client = new MongoClient(new ServerAddress(uri, Integer.valueOf(port)));
				db = this.client.getDatabase(DB);
			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}
		}
		return true;
	}
	@Override
	public void close() {
		this.client.close();
	}
	@Override
	public String toString() {
		return "MongoDB";
	}

}
