package com.adp.esi.digitech.file.processing.kafka.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.adp.esi.digitech.file.processing.model.KafkaPayload;

//@Configuration
//@EnableKafka
public class KafkaConsumerListenerConfig {
	
	//@Value("${spring.kafka.bootstrap.server}")
	private String bootstrapServer;
	
	//@Value("${spring.kafka.dvts.topic.consumer.group}")
	private String consumerGroup;
	
	
	//@Bean
	public Map<String, Object> consumerConfig() {
		var map = new HashMap<String, Object>();
		map.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		map.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
		map.put(JsonSerializer.TYPE_MAPPINGS, "kafkaPayload:com.adp.esi.digitech.file.processing.model.KafkaPayload");
		map.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
	    map.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
		return map;
	}
	
	//@Bean
	public ConsumerFactory<String, KafkaPayload> consumerFactory(){
		return new DefaultKafkaConsumerFactory<>(consumerConfig());
	}
	
	//@Bean("containerFactory")
	public ConcurrentKafkaListenerContainerFactory<String, KafkaPayload> containerFactory(){
		var factory = new ConcurrentKafkaListenerContainerFactory<String, KafkaPayload>();		
		factory.setConsumerFactory(consumerFactory());
		factory.setConcurrency(1);
		return factory;
	}

}
