package com.adp.esi.digitech.file.processing.kafka.message.service;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.file.processing.autowire.service.CustomProcessorDynamicAutowireService;
import com.adp.esi.digitech.file.processing.enums.ProcessType;
import com.adp.esi.digitech.file.processing.exception.ConfigurationException;
import com.adp.esi.digitech.file.processing.exception.GenerationException;
import com.adp.esi.digitech.file.processing.exception.ReaderException;
import com.adp.esi.digitech.file.processing.exception.TransformationException;
import com.adp.esi.digitech.file.processing.exception.ValidationException;
import com.adp.esi.digitech.file.processing.model.KafkaPayload;
import com.adp.esi.digitech.file.processing.model.RequestContext;
import com.adp.esi.digitech.file.processing.processor.service.SharedFilesAsyncProcessorService;

import lombok.extern.slf4j.Slf4j;

//@Service
@Slf4j
public class KafkaConsumerMessagingListenerService {
	
	//@Autowired
	CustomProcessorDynamicAutowireService customProcessorDynamicAutowireService;
	
	/*
	@KafkaListener(topics = "${spring.kafka.dvts.topic.name}", 
				   containerFactory = "containerFactory", 
				   id = "${spring.kafka.dvts.topic.id}")	
	public void consumeMessage(
			@Payload KafkaPayload kafkaPayload,
			@Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
			@Header(KafkaHeaders.RECEIVED_TOPIC) String topic, 			
			@Header(KafkaHeaders.OFFSET) String offset
			) {
		//@Header(KafkaHeaders.RECEIVED_KEY) String key
		log.info("Recived message from kafka, for the topic - {}, with the partition - {}, key - {}, and the offset is {}",topic, partition, 0, offset);
		log.info("data = {}",  kafkaPayload);
		
		try {
			processMessage(kafkaPayload);
		} catch (InterruptedException e) {
			
		}
		
	}
	*/
	
	
	private void processMessage(KafkaPayload kafkaPayload) throws InterruptedException {
		var request = kafkaPayload.getPayload();
		RequestContext requestContext = new RequestContext();
		requestContext.setBu(request.getBu());
		requestContext.setPlatform(request.getPlatform());
		requestContext.setDataCategory(request.getDataCategory());
		requestContext.setUniqueId(request.getUniqueId());
		requestContext.setRequestUuid(kafkaPayload.getRequestUuid());
		requestContext.setSaveFileLocation(request.getSaveFileLocation());
		try {
			customProcessorDynamicAutowireService.processAsync(SharedFilesAsyncProcessorService.class, kafkaPayload.getPayload(), requestContext);
		} catch (ReaderException | ConfigurationException | ValidationException | TransformationException
				| GenerationException e) {
			
		} catch(Exception e) {
			
		}
	}
}
