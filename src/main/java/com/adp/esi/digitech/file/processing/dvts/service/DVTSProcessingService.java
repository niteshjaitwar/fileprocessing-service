package com.adp.esi.digitech.file.processing.dvts.service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import com.adp.esi.digitech.file.processing.dvts.config.DVTSConfiguration;
import com.adp.esi.digitech.file.processing.dvts.dto.DVTSProcessingResponseDTO;
import com.adp.esi.digitech.file.processing.dvts.dto.DataPayload;
import com.adp.esi.digitech.file.processing.exception.DataValidationException;
import com.adp.esi.digitech.file.processing.exception.ProcessException;
import com.adp.esi.digitech.file.processing.exception.TransformationException;
import com.adp.esi.digitech.file.processing.model.Row;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

@Service
@Slf4j
public class DVTSProcessingService {
	
	@Autowired
	@Qualifier("dvtsWebClient")
	private WebClient webClient;
	
	@Autowired
	ObjectMapper objectMapper;
	
	@Autowired
	private DVTSConfiguration dvtsConfiguration;
	
	@Value("${request.window.timeout.seconds}")
	int windowtimeoutSeconds;
	
	
	public Mono<DVTSProcessingResponseDTO> processValidations(DataPayload payload) throws DataValidationException {		
		return webClient.post()
		.uri(dvtsConfiguration.getValidationURI())
		.bodyValue(compress(payload))
		.retrieve()
		.bodyToMono(DVTSProcessingResponseDTO.class)
		.timeout(Duration.ofSeconds(windowtimeoutSeconds));
	}
	
	public Mono<DVTSProcessingResponseDTO> processTransformations(DataPayload payload) throws TransformationException {		
		return webClient.post()
		.uri(dvtsConfiguration.getTransformationURI())
		.bodyValue(compress(payload))
		.retrieve()
		.bodyToMono(DVTSProcessingResponseDTO.class)
		.timeout(Duration.ofSeconds(windowtimeoutSeconds));		
		//log.info("DVTSProcessingService -> processTransformations(), Completed transformations status = {}", response.getStatus().getStatus());
	}
	
	public Mono<DVTSProcessingResponseDTO> processData(DataPayload payload) throws DataValidationException, TransformationException, ProcessException {		
		return webClient.post()
		.uri(dvtsConfiguration.getProcessURI())
		.bodyValue(compress(payload))
		.retrieve()
		.bodyToMono(DVTSProcessingResponseDTO.class)
		.timeout(Duration.ofSeconds(windowtimeoutSeconds));
	}
	
	private byte[] compress(DataPayload payload) throws ProcessException{
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		
		try(GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream)) {
			var json = objectMapper.writeValueAsString(payload);
			gzipOutputStream.write(json.getBytes(StandardCharsets.UTF_8));
		} catch (IOException e) {
			throw new ProcessException("", e);
		}
		return byteArrayOutputStream.toByteArray();
	}
}
