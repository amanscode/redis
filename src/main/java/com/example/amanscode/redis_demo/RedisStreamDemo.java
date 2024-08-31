package com.example.amanscode.redis_demo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.lettuce.core.Consumer;
import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;

public class RedisStreamDemo {

	public static void main(String[] args) {
		RedisURI redisURI = RedisURI.create("redis://127.0.0.1:6379");
		RedisClient redisClient = RedisClient.create(redisURI);
		StatefulRedisConnection<String, String> connection = redisClient.connect();
		RedisCommands<String, String> syncCommands = connection.sync();
		RedisAsyncCommands<String, String> asyncCommands = connection.async();
		
		System.out.println("Flushing All for a fresh start: " + syncCommands.flushall()); System.out.println();
		String streamName = "aman";
		Map<String, String> data;
		
		data = new HashMap<String, String>();
		data.put("Key1", "Value1");
		String response1 = addXAdd(syncCommands, streamName, 3L, data, "Added at number 1");
		
		data = new HashMap<String, String>();
		data.put("Key2", "Value2"); data.put("Key21", "Value21");
		String response2 = addXAdd(syncCommands, streamName, 3L, data, "Added at number 2");
		
//		data = new HashMap<String, String>();
//		data.put("Key3", "Value3");
//		String response3 = addXAdd(syncCommands, streamName, 3L, data, "Added at number 3");
		
//		RedisStreamDemo.readXRange(syncCommands, streamName, "-", "+");
//		
//		RedisStreamDemo.delete(syncCommands, streamName, response1);
//		
//		RedisStreamDemo.readXRange(syncCommands, streamName, "-", "+");
		
		StreamOffset<String> streamOffsetConsumer = StreamOffset.from(streamName, "0-0");
		String cgroup1 = "cgroup1";
		System.out.println("- Creating consumer group consumer1 : " + syncCommands.xgroupCreate(streamOffsetConsumer, cgroup1));
		String cgroup2 = "cgroup2";
		System.out.println("- Creating consumer group consumer1 : " + syncCommands.xgroupCreate(streamOffsetConsumer, cgroup2));
		Consumer<String> Ram = Consumer.from(cgroup1, "Ram");
		Consumer<String> Shiv = Consumer.from(cgroup1, "Shiv");
		Consumer<String> Rahul = Consumer.from(cgroup1, "Rahul");
		RedisStreamDemo.readXGroup(syncCommands, streamName, Ram, ">");
//		
//		data = new HashMap<String, String>();
//		data.put("Key3", "Value3");
//		String response3 = addXAdd(syncCommands, streamName, 3L, data, "Added at number 3");
//		RedisStreamDemo.readXGroup(syncCommands, streamName, Shiv, ">");
		
//		System.out.println();
//		Map<Object, Object> streamData = RedisStreamDemo.infoStream(syncCommands, streamName);
//		if(streamData != null) {
//			System.out.println("Data for " + streamName);
//			for(Map.Entry<Object, Object> entry : streamData.entrySet()) {
//				System.out.println(entry.getKey() + " == " + entry.getValue());
//			}
//		}
//		else {
//			System.out.println("Stream " + streamName + " does not exist.");
//		}
//		
//		System.out.println();
//		streamData = RedisStreamDemo.infoStream(syncCommands, "aa");
//		if(streamData != null) {
//			System.out.println("Data for " + "aa");
//			for(Map.Entry<Object, Object> entry : streamData.entrySet()) {
//				System.out.println(entry.getKey() + " == " + entry.getValue());
//			}
//		}
//		else {
//			System.out.println("Stream " + "aa" + " does not exist.");
//		}
		
//		Map<String, Map<String, String>> groups = RedisStreamDemo.infoStreamGroups(syncCommands, streamName);
//		System.out.println("groups size : " + groups.size()); System.out.println();
//		for(Map.Entry<String, Map<String, String>> e : groups.entrySet()) {
//			String groupName = e.getKey();
//			System.out.println("Details for " + groupName);
//			Map<String, String> groupDetails = e.getValue();
//			for(Map.Entry<String, String> entry : groupDetails.entrySet()) {
//				System.out.println(entry.getKey() + " = " + entry.getValue());
//			}
//			System.out.println();
//		}
		
//		Map<String, Map<String, String>> groups = RedisStreamDemo.infoStreamConsumers(syncCommands, streamName, cgroup1);
//		System.out.println("groups size : " + groups.size()); System.out.println();
//		for(Map.Entry<String, Map<String, String>> e : groups.entrySet()) {
//			String groupName = e.getKey();
//			System.out.println("Details for " + groupName);
//			Map<String, String> groupDetails = e.getValue();
//			for(Map.Entry<String, String> entry : groupDetails.entrySet()) {
//				System.out.println(entry.getKey() + " = " + entry.getValue());
//			}
//			System.out.println();
//		}
		
//		MyConsumer cRam = new MyConsumer(syncCommands, streamName, cgroup1, Ram, "Ram");
//		MyConsumer cShiv = new MyConsumer(syncCommands, streamName, cgroup1, Shiv, "Shiv");
//		MyConsumer cRahul = new MyConsumer(syncCommands, streamName, cgroup1, Rahul, "Rahul");
//		
//		RedisStreamDemo.readXGroup(syncCommands, streamName, Ram, ">");
//		
//		RedisStreamDemo.readXGroup(syncCommands, streamName, Shiv, ">");
//		RedisStreamDemo.readXGroup(syncCommands, streamName, Shiv, "0-0");
//		
//		RedisStreamDemo.claimMessages(syncCommands, streamName, cgroup1, Shiv, 0L, response1);
//		RedisStreamDemo.claimMessages(syncCommands, streamName, cgroup1, Shiv, 0L, response2);
//		
//		RedisStreamDemo.readXGroup(syncCommands, streamName, Shiv, "0-0");
//		
//		RedisStreamDemo.readXGroup(syncCommands, streamName, Ram, ">");
//		
//		List<StreamMessage<String, String>> xreadgroupShiv = RedisStreamDemo.readXGroup(syncCommands, streamName, Shiv, "0-0");
//		List<StreamMessage<String, String>> xreadgroupRam = RedisStreamDemo.readXGroup(syncCommands, streamName, Ram, "0-0");
//
//		System.out.println();
//		RedisStreamDemo.ackMessages(syncCommands, streamName, xreadgroupRam);
//		System.out.println();
//		RedisStreamDemo.ackMessages(syncCommands, streamName, xreadgroupShiv);
//		
//		System.out.println();
//		xreadgroupShiv = RedisStreamDemo.readXGroup(syncCommands, streamName, Shiv, "0-0");
//		System.out.println();
//		xreadgroupRam = RedisStreamDemo.readXGroup(syncCommands, streamName, Ram, "0-0");
//		
//		System.out.println();
//		RedisStreamDemo.claimMessages(syncCommands, streamName, cgroup1, Shiv, 0L, response3);
//		
//		data = new HashMap<String, String>();
//		data.put("Key1", "Value1"); data.put("Key4", "Value4");
//		String response4 = addXAdd(syncCommands, streamName, 3L, data, "Added at number 4");
//		
//		data = new HashMap<String, String>();
//		data.put("Key1", "Value1"); data.put("Key5", "Value5");
//		String response5 = addXAdd(syncCommands, streamName, 3L, data, "Added at number 5");
	}
	
	public static String addXAdd(RedisCommands<String, String> syncCommands, String streamName, Long maxLen, Map<String, String> data, String logMessage) {
		XAddArgs xaddArg = new XAddArgs();
		xaddArg.maxlen(maxLen);
		String response = syncCommands.xadd(streamName, xaddArg, data);
		System.out.println("Response of XADD with maxlen :  " + logMessage + " :: " + response);
		return response;
	}
	
	public static Long delete(RedisCommands<String, String> syncCommands, String streamName, String message) {
		System.out.println(); System.out.println("Deleting message : " + message);
		Long response = syncCommands.xdel(streamName, message);
		System.out.println("Response of XDEL with maxlen :  " + response); System.out.println();
		return response;
	}
	
	public static List<StreamMessage<String, String>> readXRange(RedisCommands<String, String> syncCommands, String streamName, String start, String end) {
		System.out.println(); System.out.println("Reading everything using XRANGE between " + start + " & " + end + " **********************");
		Range<String> range = Range.create(start, end);
		Limit limit = Limit.from(20L);
		List<StreamMessage<String, String>> streamMessagesList = syncCommands.xrange(streamName, range, limit);
		int listSize = streamMessagesList.size();
		for (int i = 0; i < listSize; i++) {
            StreamMessage<String, String> streamMessage = streamMessagesList.get(i);
            String _streamName = streamMessage.getStream();
            String _streamID = streamMessage.getId();
            System.out.println(_streamName + " : " + _streamID);
            Map<String, String> _data = streamMessage.getBody();
            for(Map.Entry<String, String> entry : _data.entrySet()) {
            	System.out.println("Field: " + entry.getKey() + ", Value: " + entry.getValue());
            }
        }
		return streamMessagesList;
	}
	
	public static List<StreamMessage<String, String>> readXGroup(RedisCommands<String, String> syncCommands, String streamName, Consumer<String> consumerName, String offset){
		System.out.println(); System.out.println("Reading latest unACK data by " + consumerName.getName() + " with offset as " + offset + "..."); System.out.println();
		List<StreamMessage<String, String>> xreadgroup;
		StreamOffset<String> streamOffsetConsumer = StreamOffset.from(streamName, offset);
		xreadgroup = syncCommands.xreadgroup(consumerName, streamOffsetConsumer);
		int listSize = xreadgroup.size(); System.out.println("listSize : " + listSize);
		for (int i = 0; i < listSize; i++) {
            StreamMessage<String, String> streamMessage = xreadgroup.get(i);
            String _streamName = streamMessage.getStream();
            String _streamID = streamMessage.getId();
            System.out.println(_streamName + " : " + _streamID);
            Map<String, String> _data = streamMessage.getBody();
            for(Map.Entry<String, String> entry : _data.entrySet()) {
            	System.out.println("Field: " + entry.getKey() + ", Value: " + entry.getValue());
            }
        }
		return xreadgroup;
	}
	
	public static void ackMessages(RedisCommands<String, String> syncCommands, String streamName, List<StreamMessage<String, String>> xreadgroup) {
		System.out.println(); System.out.println("ACK messages **********************"); System.out.println();
		int listSize = xreadgroup.size();
		for (int i = 0; i < listSize; i++) {
            StreamMessage<String, String> streamMessage = xreadgroup.get(i);
            String _streamName = streamMessage.getStream();
            String _streamID = streamMessage.getId();
            System.out.println("Ack'ing message : " + _streamID);
            syncCommands.xack(streamName, "consumer1", _streamID);
        }
	}
	
//	public static void pendingMessages(RedisCommands<String, String> syncCommands, String streamName, String consumerGroupName, String start, String end, Long limit) {
//		System.out.println(); System.out.println("XPENDING **********************"); System.out.println();
//		List pendingList = syncCommands.xpending(streamName, consumerGroupName);
//		System.out.println(pendingList);
//		List<Object> pendingListDetailed = syncCommands.xpending(streamName, consumerGroupName, Range.create(start, end), Limit.from(limit));
//		System.out.println(pendingListDetailed);
//		for(int i = 0; i < pendingListDetailed.size(); i++)
//		{
//			List n = (List)pendingListDetailed.get(i);
//			System.out.println("Pending Message " + i + " : " + n);
//		}
//	}
	
//	public static void claimMessages(RedisCommands<String, String> syncCommands, String streamName, String consumerGroupName, Consumer consumer, Long idleTime, String messageID) {
//		System.out.println(); System.out.println("Claiming message for " + consumer.getName() + " with ID " + messageID + " **********************"); System.out.println();
//		List pendingList = syncCommands.xpending(streamName, consumerGroupName);
//		System.out.println(pendingList);
//		List<StreamMessage<String, String>> xclaimgroup = syncCommands.xclaim(streamName, consumer, idleTime, messageID);
//		int listSize = xclaimgroup.size(); System.out.println("xclaimgroup listSize : " + listSize);
//		for (int i = 0; i < listSize; i++) {
//            StreamMessage<String, String> streamMessage = xclaimgroup.get(i);
//            String _streamName = streamMessage.getStream();
//            String _streamID = streamMessage.getId();
//            System.out.println(_streamName + " : " + _streamID);
//            Map<String, String> _data = streamMessage.getBody();
//            for(Map.Entry<String, String> entry : _data.entrySet()) {
//            	System.out.println("Field: " + entry.getKey() + ", Value: " + entry.getValue());
//            }
//        }
//	}

	public static Map<Object, Object> infoStream(RedisCommands<String, String> syncCommands, String streamName) {
		List<Object> list = null;
		boolean streamPresent = false;
		try{
			list = syncCommands.xinfoStream(streamName);
			streamPresent = true;
		}
		catch(RedisCommandExecutionException e){
			String exceptionMessage = e.getMessage();
			if("ERR no such key".equalsIgnoreCase(exceptionMessage.trim())) {
				System.out.println("Stream " + streamName + " not present.");
			}
		}
		if(streamPresent == false) return null;
		
		Map<Object, Object> streamData = new HashMap<Object, Object>();
		for (int i = 0; i < list.size(); i=i+2) {
			streamData.put(list.get(i).toString(), list.get(i+1).toString());
		}
		
		return streamData;
	}
	
	public static Map<String, Map<String, String>> infoStreamGroups(RedisCommands<String, String> syncCommands, String streamName) {
		List<Object> listGroups = syncCommands.xinfoGroups(streamName);
		Map<String, Map<String, String>> groups = new HashMap<String, Map<String, String>>();
		for (int i = 0; i < listGroups.size(); i++) {
			List<Object> groupData = (List)listGroups.get(i);
			Map<String, String> groupMap = new HashMap<String, String>();
			for (int j = 0; j < groupData.size(); j=j+2) {
				groupMap.put(groupData.get(j).toString(), groupData.get(j+1).toString());
			}
			groups.put(groupMap.get("name"), groupMap);
		}
		return groups;
	}
	
	public static Map<String, Map<String, String>> infoStreamConsumers(RedisCommands<String, String> syncCommands, String streamName, String groupName) {
		List<Object> listGroups = syncCommands.xinfoConsumers(streamName, groupName);
		Map<String, Map<String, String>> groups = new HashMap<String, Map<String, String>>();
		for (int i = 0; i < listGroups.size(); i++) {
			List<Object> groupData = (List)listGroups.get(i);
			Map<String, String> groupMap = new HashMap<String, String>();
			for (int j = 0; j < groupData.size(); j=j+2) {
				groupMap.put(groupData.get(j).toString(), groupData.get(j+1).toString());
			}
			groups.put(groupMap.get("name"), groupMap);
		}
		return groups;
	}
	
}


