package com.example.amanscode.redis_demo;

import io.lettuce.core.Consumer;
import io.lettuce.core.api.sync.RedisCommands;

public class MyConsumer extends Thread{
	
	RedisCommands<String, String> syncCommands;
	String streamName;
	String group;
	Consumer<String> consumer;
	String name;
	
	public MyConsumer(RedisCommands<String, String> syncCommands, String streamName, String group, Consumer<String> consumer, String name){
		this.syncCommands = syncCommands;
		this.streamName = streamName;
		this.group = group;
		this.consumer = consumer;
		this.name = name;
	}
	
	@Override
	public void run() {
		/*
		 * try { this.wait(5000L); } catch (InterruptedException e) {
		 * System.out.println("Error : Consumer: " + this.consumer.getName());
		 * e.printStackTrace(); }
		 */
		this.consume();
	}
	
	private void consume() {
		RedisStreamDemo.readXGroup(syncCommands, streamName, this.consumer, ">");
	}
}
