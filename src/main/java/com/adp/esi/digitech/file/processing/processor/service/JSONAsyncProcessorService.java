package com.adp.esi.digitech.file.processing.processor.service;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import com.adp.esi.digitech.file.processing.autowire.service.CustomProcessorDynamicAutowireService;
import com.adp.esi.digitech.file.processing.enums.ProcessType;
import com.adp.esi.digitech.file.processing.enums.Status;
import com.adp.esi.digitech.file.processing.exception.ConfigurationException;
import com.adp.esi.digitech.file.processing.exception.GenerationException;
import com.adp.esi.digitech.file.processing.exception.ProcessException;
import com.adp.esi.digitech.file.processing.exception.ReaderException;
import com.adp.esi.digitech.file.processing.exception.TransformationException;
import com.adp.esi.digitech.file.processing.exception.ValidationException;
import com.adp.esi.digitech.file.processing.model.ApiResponse;
import com.adp.esi.digitech.file.processing.model.RequestPayload;

import lombok.extern.slf4j.Slf4j;

@Service("jsonAsyncProcessorService")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class JSONAsyncProcessorService extends AbstractAsyncProcessorService<Void> {

	@Autowired
	CustomProcessorDynamicAutowireService customProcessorDynamicAutowireService;

	@Autowired
	Executor asyncExecutor;

	@Autowired
	@Qualifier("autoJobsWebClient")
	WebClient autoJobsWebClient;

	@Value("${autojobs.rest.statusUpdateURI}")
	private String statusUpdateURI;

	@Override
	public Void process(RequestPayload request) throws IOException, ReaderException, ConfigurationException,
	ValidationException, TransformationException, GenerationException {
		try {
			objectUtilsService.dataStudioConfigurationService.validateRequest(request.getBu(), request.getPlatform(), request.getDataCategory());
		} catch (ConfigurationException e) {
			e.setRequestContext(requestContext);
			throw e;
		}
		CompletableFuture.supplyAsync(() -> {
			try {
				var processResponse = customProcessorDynamicAutowireService.process(JSONProcessorService.class, request,
						requestContext, ProcessType.sync);
				return ApiResponse.success(Status.SUCCESS, "Data  processed successfully. Passed all validation rules.",
						processResponse);
			} catch (Exception e) {
				throw new ProcessException(e.getMessage(), e.getCause());
			}
		}, asyncExecutor).handle((response, e) -> {
			if (e != null) {
				log.error("JSONAsyncProcessorService - process(), Error processing async request: {}", e.getMessage());
				return handleError(e);
			}
			return response;
		}).thenAcceptAsync(response -> {
			try {
				var responseStr = objectUtilsService.objectMapper.writeValueAsString(response);
				JSONObject responseJson = new JSONObject();
				responseJson.put("status", response.getStatus().getStatus());
				responseJson.put("xrefid", request.getEcosystemId());
				responseJson.put("performer", "DVTS-System");
				responseJson.put("responseMsg", responseStr);

				
				autoJobsWebClient.post()
								 .uri(statusUpdateURI)
								 .bodyValue(responseJson.toString())
								 .retrieve()
								 .bodyToMono(String.class)								 
								 .block();
				
				log.info("JSONAsyncProcessorService - process(), Completed sending response to AutoJobs: {}",
						responseJson.toString());

			} catch (Exception e1) {
				log.error("JSONAsyncProcessorService - process(), Error on updating status to AutoJobs: {}", e1.getMessage());
			}
		}, asyncExecutor);

		return null;
	}

}
