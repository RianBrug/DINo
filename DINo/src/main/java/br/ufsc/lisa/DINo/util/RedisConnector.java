package br.ufsc.lisa.DINo.util;

import java.util.Set;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisConnector implements Connector {

	// private MongoClient client;
	private Jedis jedis;
	private JedisPool pool;
	private String db;
	private Integer port;
	private String password;

	public boolean connect(String uri, String port, String password) {
		
		try {
			jedis = new Jedis(uri, Integer.valueOf(port));
			if(password != null & !password.isEmpty())
				jedis.auth(password);
			jedis.connect();
//			jedis.close();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
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
	
	public void readRedis() {
//		connect("localhost", "6379", "");
		
		
		if(jedis != null) {
//		System.out.println(jedis.keys("*"));
		System.out.println(jedis.rpop("dinotest_92952666_4_149876")+" removido com sucesso");
		jedis.close();
	}
	}


}
