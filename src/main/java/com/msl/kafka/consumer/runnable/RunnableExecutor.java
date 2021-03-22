package com.msl.kafka.consumer.runnable;

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
	private static final String GROUP_ID = "smartprocess-runnable-cpd1";

	public static void main(String[] args) {
		log.info("Starting kafka consumers");
		startKafkaConsumersAsync();
		log.info("Kafka consumers started");
	}

	public static void startKafkaConsumersAsync() {

		int numConsumers = 3;

		List<String> topics = Arrays.asList(TOPIC);
		final ExecutorService executor = Executors.newFixedThreadPool(numConsumers);

		final List<SmartAlarmlogKafkaTopicConsumerRunnable> consumers = new ArrayList<>();
		for (int i = 0; i < numConsumers; i++) {
			SmartAlarmlogKafkaTopicConsumerRunnable consumer = new SmartAlarmlogKafkaTopicConsumerRunnable(i, GROUP_ID,
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

}
