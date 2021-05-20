package com.example.amanscode.redis_demo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;

public class SortedSetsDemo {
	public static RedisCommands<String, String> syncCommands;
	public static void main(String[] args) {
		RedisURI redisURI = RedisURI.create("redis://10.32.141.15:6002");
		RedisClient redisClient = RedisClient.create(redisURI);
		StatefulRedisConnection<String, String> connection = redisClient.connect();
		RedisCommands<String, String> syncCommandss = connection.sync();
		syncCommands = syncCommandss;
		RedisAsyncCommands<String, String> asyncCommands = connection.async();
		System.out.println("Connected.");
//		System.out.println("Flushing All for a fresh start: " + syncCommands.flushall());
		System.out.println();
		String usersSet = "users";
		
		addZadd(usersSet, 2D, "a"); addZadd(usersSet, -7D, "b"); addZadd(usersSet, 12D, "c"); addZadd(usersSet, -3D, "d");
		
//		String st = "3"; String en = "3";
//		Long start = new Long(st);
//		Long end = new Long(en);
//		Map<String, Double> data = getUsersByScores(usersSet, start-1, end-1);
//		System.out.println();
//		for(Map.Entry<String, Double> e : data.entrySet()) {
//			System.out.println(e.getKey() + " : " + e.getValue());
//		}
		
		System.out.println("score for b is " + getScoreForUser(usersSet, "b"));
	}
	
	public static Map<String, Double> getUsersByScores(String usersSet, Long start, Long end){
		Map<String, Double> data = new HashMap<String, Double>();
		List<ScoredValue<String>> list = syncCommands.zrangeWithScores(usersSet, start, end);
		for (ScoredValue<String> sv : list) {
			sv.getValue();
			sv.getScore();
			data.put(sv.getValue(), sv.getScore());
		}
		System.out.println("List : " + list);
		return data;
	}
	
	public static Double getScoreForUser(String usersSet, String user){
		Double score = syncCommands.zscore(usersSet, user);
		return score;
	}
	
	public static void addZadd(String usersSet, Double score, String key){
		Optional<String> opt = Optional.of(key);
		ScoredValue<String> s = (ScoredValue<String>) ScoredValue.from(score, opt);
		Long l = syncCommands.zadd(usersSet, s);
	}
}
