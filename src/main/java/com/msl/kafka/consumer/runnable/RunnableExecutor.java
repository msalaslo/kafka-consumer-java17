package com.msl.kafka.consumer.runnable;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

public class RunnableExecutor {

	private static final Logger log = Logger.getLogger(RunnableExecutor.class);
	private static final String TOPIC = "smartprocess.alarmlog";
	private static final String GROUP_ID = "smartprocess-runnable-standalone";

	public static void main(String[] args) {
		log.info("Starting kafka consumers");
		startKafkaConsumersAsync();
		log.info("Kafka consumers started");
	}

	public static void startKafkaConsumersAsync() {

		int numConsumers = 1;

		List<String> topics = Arrays.asList(TOPIC);
		final ExecutorService executor = Executors.newFixedThreadPool(numConsumers);

		final List<SmartAlarmlogKafkaTopicConsumerRunnable> consumers = new ArrayList<>();
		for (int i = 0; i < numConsumers; i++) {
			SmartAlarmlogKafkaTopicConsumerRunnable consumer = new SmartAlarmlogKafkaTopicConsumerRunnable(getClientId(i), GROUP_ID,
					topics);
			consumers.add(consumer);
			executor.submit(consumer);
		}

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				for (SmartAlarmlogKafkaTopicConsumerRunnable consumer : consumers) {
					consumer.shutdown();
				}
				executor.shutdown();
				try {
					executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
					log.error("Error terminating kafka consumers", e);
				}
			}
		});

	}
	
	private static String getClientId(int i) {
		String clientId = "default" + i;
		try {
			String hostAddress = InetAddress.getLocalHost().getHostAddress();
			String ip = InetAddress.getLocalHost().getHostName();
			log.info("Host name:" + hostAddress + ", ip:" + ip);
			clientId = hostAddress;
		} catch (UnknownHostException e) {
			log.warn("Error getting host name, using default clientId:" + clientId, e);
		}
		return clientId;
	}

}
