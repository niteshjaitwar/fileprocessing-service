package com.adp.esi.digitech.file.processing.kafka.message.service;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.file.processing.model.KafkaPayload;

import lombok.extern.slf4j.Slf4j;

//@Service
@Slf4j
public class KafkaProducerMessagingService {
	
	//@Autowired
	KafkaTemplate<String, KafkaPayload> kafkaTemplate;
	
	//@Value("${spring.kafka.dvts.topic.name}")
	public String topicName;
	
	
	public void publish(KafkaPayload data) {
		var resultConsumer = kafkaTemplate.send(topicName,data);
		
		resultConsumer.whenComplete((result, ex) -> {
			if(ex == null)				
				handleSuccess(result.getRecordMetadata(), data);			
			else
				handleFailure(ex, data);
		});
	}
	
	private void handleSuccess(RecordMetadata metadata, KafkaPayload data) {
		log.info("Message published successfully, where message is {} and offset is {}", data, metadata.offset());
	}
	
	private void handleFailure(Throwable ex, KafkaPayload data) {
		log.error("Unable to send message {}, due to {}", data, ex.getMessage());	
	}

}
