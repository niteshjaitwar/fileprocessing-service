package com.adp.esi.digitech.file.processing.request.service;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.adp.esi.digitech.file.processing.autowire.service.CustomProcessorDynamicAutowireService;
import com.adp.esi.digitech.file.processing.entity.FPSRequestQueueEntity;
import com.adp.esi.digitech.file.processing.enums.Status;
import com.adp.esi.digitech.file.processing.exception.ProcessException;
import com.adp.esi.digitech.file.processing.model.RequestContext;
import com.adp.esi.digitech.file.processing.model.RequestPayload;
import com.adp.esi.digitech.file.processing.processor.service.SharedFilesAsyncProcessorService;
import com.adp.esi.digitech.file.processing.repo.FPSRequestQueueRepository;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class FPSRequestQueueService {
	
	@Autowired
	FPSRequestQueueRepository fpsRequestQueueRepository;
	
	@Autowired
	CustomProcessorDynamicAutowireService customProcessorDynamicAutowireService;
	
	@Autowired
	ObjectMapper objectMapper;
	
	private final AtomicBoolean isProcessing = new AtomicBoolean(false);
	
	@Scheduled(fixedDelay = 30000)
	public void processRequestQueue() {		
		if(isProcessing.compareAndSet(false, true)) {
			try {
				fetchAndProcessNextMessage();
			} finally {
				isProcessing.set(false);
			}
		}
	}
	
	@Transactional
	public void fetchAndProcessNextMessage() {
		var requestOpt = fpsRequestQueueRepository.fetchNextPendingMessage(Status.PENDING.getStatus());
		requestOpt.ifPresent(request -> {
			updateMesasgeStatus(request.getId(), Status.PROCESSING.getStatus());
			processMessage(request);
		});
	}
	
	
	public void processMessage(FPSRequestQueueEntity request) {
		try {
			
			var payload = objectMapper.readValue(request.getRequestPayload(), RequestPayload.class);	
			var requestContext = new RequestContext();
			requestContext.setBu(payload.getBu());
			requestContext.setPlatform(payload.getPlatform());
			requestContext.setDataCategory(payload.getDataCategory());
			requestContext.setUniqueId(payload.getUniqueId());
			requestContext.setRequestUuid(request.getUuid());
			requestContext.setSaveFileLocation(payload.getSaveFileLocation());
			
			customProcessorDynamicAutowireService.processAsync(SharedFilesAsyncProcessorService.class, payload, requestContext);
			updateMesasgeStatus(request.getId(), Status.COMPLETED.getStatus());
		} catch (IOException | ProcessException e) {
			log.error("FPSRequestQueueService -> processMessage(), processing failed with error = {}", e.getMessage());
			updateMesasgeStatus(request.getId(), Status.FAILED_PROCESSING.getStatus());
		}  catch (Exception e) {
			log.error("FPSRequestQueueService -> processMessage(), Failed to initiate process, error = {}", e.getMessage());
			updateMesasgeStatus(request.getId(), Status.FAILED_PROCESSING.getStatus());
		}
	}
	
	
	public void updateMesasgeStatus(Long mesasgeId, String status) {
		fpsRequestQueueRepository.updateMessageStatus(mesasgeId, status);
	}
	
	@Transactional
	public void save(RequestPayload request, String requestUuid) throws IOException {
		var entity = FPSRequestQueueEntity.builder()
		.uniqueId(request.getUniqueId())
		.uuid(requestUuid)
		.status(Status.PENDING.getStatus())
		.createdDate(new Date())
		.requestPayload(objectMapper.writeValueAsString(request)).build();		
		fpsRequestQueueRepository.save(entity);
	}
	
	public FPSRequestQueueEntity findBy(String uniqueId, String uuid) {
		return fpsRequestQueueRepository.findByUniqueIdAndUuid(uniqueId, uuid).orElse(null);
	}

}
