package com.msl.kafka.consumer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;

import com.verisure.vcp.smartprocess.avro.AlarmlogDTO;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

public class SmartAlarmlogKafkaTopicConsumer {

	private static final Logger log = Logger.getLogger(SmartAlarmlogKafkaTopicConsumer.class);
	private static final String TOPIC = "smartprocess.alarmlog";
	private static final String BOOTSTRAP_SERVERS = "ef1kafkabrk01v:9092";
	private static final String GROUP_ID = "smartprocess-event-listener-cpd1";
	private static final String SCHEMA_REGISTRY_URL = "http://ef1kafkareg01v:8081";

	public static void main(String args[]) {
		KafkaConsumer<String, AlarmlogDTO> consumer = null;
		try {
			Properties props = new Properties();
			props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
			props.put("group.id", GROUP_ID);
			props.put("clientId.id", getClientId());
			props.put("enable.auto.commit", "true");
			props.put("auto.commit.interval.ms", "1000");
			props.put("key.deserializer", StringDeserializer.class);
			props.put("value.deserializer", KafkaAvroDeserializer.class);

			// Authentication: SASL_PLAINTEXT + PLAIN + JAAS
			props.put("security.protocol", "SASL_PLAINTEXT");
			props.put("sasl.mechanism", "PLAIN");
			props.put("sasl.jaas.config",
					"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"client\" password=\"client-secret\";");

			props.put("schema.registry.url", SCHEMA_REGISTRY_URL);

			// ensures records are properly converted to the SpecificRecord: AlarmlogDTO
			props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

			// NO DEJAR EN PRO, SOLO PARA PRUEBAS (Lee desde el primer mensaje)
//			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

			consumer = new KafkaConsumer<>(props);

			consumer.subscribe(Arrays.asList(TOPIC));
			while (true) {
				ConsumerRecords<String, AlarmlogDTO> records = consumer.poll(10);
				for (ConsumerRecord<String, AlarmlogDTO> record : records) {
					AlarmlogDTO alarmlogDto = record.value();
					int sinc = alarmlogDto.getSINC();
					int sins = alarmlogDto.getSINS();
					log.info("offset = " + record.offset() + ", key = " + record.offset() + ", SINS = " + sins
							+ ", SINC = " + sinc);

				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			log.error(e);
		} finally {
			if (consumer != null) {
				consumer.close();
			}
		}
	}
	
	private static String getClientId() {
		String clientId = "default";
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
