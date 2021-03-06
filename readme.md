# Kafka Consumer compatible with Java 1.7

## About

This is a simple project that consumes messages from a Kafka Topic using Java 1.7 and Kafka 0.10.
Generates an AVRO SpecificRecord with avro-maven-plugin

## Technical Stack:

- Java 1.7
- Maven 3.6+
- Kafka 0.10.2.2 (Last Kafka version compatible with Java 1.7)
- Kafka artifact Id  2.12
- AVRO 1.8.1
- AVRO Maven plugin 1.8.1
- Confluent Serdes 4.1.4 (Last Confluent version compatible with Java 1.7)


## Installation
This application is a Kafka Consumer:
- Install a Confluent platform (any version is compatible), at least one broker, one Zookeeper ad One Schema Registry is needed 
- Create a topic smartprocess.alarmlog
- In that topic create the Schema included in this project in the folder resources/avro
- Start the project by using the Main Application or or installing the jar and start up it with java -jar jar-name
- Produce messages to the topic

## Considerations:
- If you use Eclipse IDE you can see the following error:
	"Plugin execution not covered by lifecycle configuration: org.apache.avro:avro-maven-plugin:1.8.1:schema (execution: default, phase: generate-sources)	pom.xml	/kafka-consumer-java17	line 53	Maven Project Build Lifecycle Mapping Problem"
  This is because the M2E plug-in bugs: https://www.eclipse.org/m2e/documentation/m2e-execution-not-covered.html
  But it works fine.
	

