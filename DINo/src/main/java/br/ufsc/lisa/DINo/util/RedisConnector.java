package br.ufsc.lisa.DINo.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

public class RedisConnector implements Connector {

	// private MongoClient client;
	private Jedis jedis;
	private JedisPool pool;
	private String db;
	private Integer port;
	private String password;
	private String url;

	public boolean connect(String uri, String port, String password) {
		
		try {
			jedis = new Jedis(uri, Integer.valueOf(port));
			if(password != null & !password.isEmpty())
				jedis.auth(password);
			jedis.connect();
			jedis.close();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		this.url = uri;
		db = uri;
		this.port = Integer.valueOf(port);
		this.password =  password;
//		jedis = pool.getResource();
		return true;
		
	}

	@Override
	public boolean put(String key, String value) {
		if (jedis != null) {
			String rkey = key;
			String rvalue = value.toString();
			jedis.rpush(rkey, rvalue);
			 jedis.close();
			return true;
		}
		return false;
	}
	
	@Override
	public void close() {
		this.jedis.close();
	}

	@Override
	public String toString() {
		return "Redis DB";
	}

	@Override
	public boolean connect(String uri, String port, String user, String password, String DB) {
		return this.connect(uri, port, password);
	}
	
	public void createSchema(String key) {
		Map<String, JSON> schema = new HashMap<String, JSON>();
	}
	
	public void readRedis() {
//		String porta = port.toString();
		connect("localhost", "6379", "");
		if(jedis != null) {
			ScanParams params = new ScanParams();
			params.match("*rian*");

			// An iteration starts at "0": http://redis.io/commands/scan
			ScanResult<String> scanResult = jedis.scan("0", params);
			List<String> keys = scanResult.getResult();
			String nextCursor = scanResult.getStringCursor();
			int counter = 0;

			while (true) {
			    for (String key : keys) {
			        createSchema(key);
			        System.out.println(nextCursor + "nextCursor");
			    }

			    // An iteration also ends at "0"
			    if (nextCursor.equals("0")) {
			        break;
			    }

			    scanResult = jedis.scan(nextCursor, params);
			    nextCursor = scanResult.getStringCursor();
			    keys = scanResult.getResult();
			    System.out.println(keys = scanResult.getResult());
			}
		}
		jedis.close();
		
	}


}
