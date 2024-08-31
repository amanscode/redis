package com.example.amanscode.redis_demo;

import java.time.Duration;
import java.util.List;

import io.lettuce.core.KeyValue;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;

public class RedisClusterDemo {
	public static void main(String[] args) {
//		RedisClusterClient redisClient = RedisClusterClient.create("redis://10.32.141.4:6379,10.32.141.5:6379,10.32.141.6:6379");
		RedisClusterClient redisClient = RedisClusterClient.create("redis://10.32.141.15:7770,10.32.141.15:7771,10.32.141.15:7772");
//		RedisClusterClient redisClient = RedisClusterClient.create("redis://10.32.141.15:7770");

		ClusterTopologyRefreshOptions topologyRefreshOptions = ClusterTopologyRefreshOptions.builder()
				.enablePeriodicRefresh(Duration.ofSeconds(15)).enableAllAdaptiveRefreshTriggers()
				.closeStaleConnections(true).build();
		redisClient.setOptions(ClusterClientOptions.builder().topologyRefreshOptions(topologyRefreshOptions).build());

		StatefulRedisClusterConnection<String, String> connection = redisClient.connect();
		RedisAdvancedClusterCommands<String, String> syncCommands = connection.sync();
		RedisAdvancedClusterAsyncCommands<String, String> asyncCommands = connection.async();
		// Rest is same as the standalone mode in lettuce!
		System.out.println("RETURN: " + syncCommands.set("a", "1"));
		System.out.println("RETURN: " + syncCommands.set("b", "1"));
//		RedisStringOpertaions(syncCommands);
		try {
			Thread.sleep(5000L);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void RedisStringOpertaions(RedisAdvancedClusterCommands<String, String> syncCommands) {
		System.out.println("- Setting String datatype in Redis as 'a=1'...");
		System.out.println("RETURN: " + syncCommands.set("a", "1"));
		System.out.println("- Getting value of above field 'a'...");
		System.out.println("RETURN: " + syncCommands.get("a"));
		System.out.println("- Appending Ram inside 'a'");
		System.out.println("RETURN: " + syncCommands.append("a", "Ram"));
		System.out.println("- Getting value of above field 'a' ");
		System.out.println("RETURN: " + syncCommands.get("a"));
		System.out.println("- Getting all the live keys...");
		System.out.println("RETURN: " + syncCommands.keys("*"));
		try {
			System.out.println("- Increase value of 'a' using 'incrby'...");
			System.out.println("RETURN: " + syncCommands.incr("a"));
		} catch (Exception e) {
			System.out.println("Exception Caught : " + e.getMessage());
		}
		System.out.println("- Getting range for 'a' from 0 to 2...");
		System.out.println("RETURN: " + syncCommands.getrange("a", 0, 2));
		System.out.println("- Getting BITCOUNT for 'a'...");
		System.out.println("RETURN: " + syncCommands.bitcount("a"));
		System.out.println("- Setting String datatype in Redis as 'a=1'...");
		System.out.println("RETURN: " + syncCommands.set("a", "1"));
		System.out.println("- Setting String datatype in Redis as 'b=2'...");
		System.out.println("RETURN: " + syncCommands.set("b", "2"));
		System.out.println("- Getting 'a' & 'b' using MGET...");
		List<KeyValue<String, String>> kv = syncCommands.mget("a", "b");
		System.out.println("RETURN: " + kv);
		System.out.println("Getting Keys and Values from above KeyValue pairs...");
		for (int i = 0; i < kv.size(); i++) {
			System.out.println(kv.get(i).getKey() + " = " + kv.get(i).getValue());
		}
	}

}
