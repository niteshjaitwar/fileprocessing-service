package com.adp.esi.digitech.file.processing.kafka.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.adp.esi.digitech.file.processing.model.KafkaPayload;

//@Configuration
public class KafkaProducerConfig {

	//@Value("${spring.kafka.bootstrap.server}")
	private String bootstrapServer;
	
	@Bean
	public Map<String, Object> producerConfig() {
		var map = new HashMap<String, Object>();
		map.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		map.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		map.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		map.put(JsonSerializer.TYPE_MAPPINGS, "kafkaPayload:com.adp.esi.digitech.file.processing.model.KafkaPayload");
		return map;
	}
	
	//@Bean
	public ProducerFactory<String, KafkaPayload> producerFactory(){
		return new DefaultKafkaProducerFactory<>(producerConfig());
	}
	
	//@Bean
	public KafkaTemplate<String, KafkaPayload> kafkaTemplate(){
		return new KafkaTemplate<>(producerFactory());
	}
}
