package com.adp.esi.digitech.file.processing.ds.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;

import com.adp.esi.digitech.file.processing.ds.config.DataStudioConfiguration;
import com.adp.esi.digitech.file.processing.ds.config.model.TargetDataFormat;
import com.adp.esi.digitech.file.processing.ds.dto.DSResponseDTO;
import com.adp.esi.digitech.file.processing.ds.model.ColumnConfiguration;
import com.adp.esi.digitech.file.processing.ds.model.ColumnRelation;
import com.adp.esi.digitech.file.processing.ds.model.ConfigurationData;
import com.adp.esi.digitech.file.processing.ds.model.TransformationRule;
import com.adp.esi.digitech.file.processing.ds.model.ValidationRule;
import com.adp.esi.digitech.file.processing.exception.ConfigurationException;
import com.adp.esi.digitech.file.processing.model.ErrorData;

import reactor.core.publisher.Mono;

@Service
public class DatastudioConfigurationService {
	
	@Autowired
	@Qualifier("dataStudioWebClient")	
	private WebClient webClient;
	
	private static final String BU = "bu";
	private static final String PLATFORM = "platform";
	private static final String DATA_CATEGORY = "dataCategory";
	private static final String RULE_TYPE = "ruleType";
	private static final String LOV_TYPE = "type";
	private static final String DATASET_ID = "dataSetId";
	
	
	@Autowired
	DataStudioConfiguration dataStudioConfiguration;
	
	public Properties findAllProperties(String type) {	
		
		var response = webClient.post()
				 .uri(dataStudioConfiguration.getLovURI())
				 .body(BodyInserters
						 .fromFormData(LOV_TYPE, type))
				 .retrieve()
				 .bodyToMono(new ParameterizedTypeReference<DSResponseDTO<Properties>>() {}).block();
		return response.getData();
	}
	
	
	public ConfigurationData findConfigurationDataBy(String bu, String platform, String dataCategory) {
		var response = webClient.post()
				.uri(dataStudioConfiguration.getConfigurationURI())
				.body(BodyInserters
						.fromFormData(BU, bu)
						.with(PLATFORM, platform)
						.with(DATA_CATEGORY, dataCategory))
				.retrieve()
				.bodyToMono(new ParameterizedTypeReference<DSResponseDTO<ConfigurationData>>() {})
				.block();
		return response.getData();
	}
	
	public Map<String,List<ColumnRelation>> findAllColumnRelationsMapBy(String bu, String platform, String dataCategory) {
		return findAllColumnRelationsBy(bu, platform, dataCategory).stream().collect(Collectors.groupingBy(ColumnRelation::getSourceKey));
	}
	
	public List<ColumnRelation> findAllColumnRelationsBy(String bu, String platform, String dataCategory) {
		var response = webClient.post()
				.uri(dataStudioConfiguration.getColumnRelationURI())
				.body(BodyInserters
						.fromFormData(BU, bu)
						.with(PLATFORM, platform)
						.with(DATA_CATEGORY, dataCategory))
				.retrieve()
				.bodyToMono(new ParameterizedTypeReference<DSResponseDTO<List<ColumnRelation>>>() {})
				.block();
		return response.getData();
	}
	
	public Map<String,List<ValidationRule>> findAllValidationRulesGroupBy(String bu, String platform, String dataCategory) {
		return findAllValidationRulesBy(bu, platform, dataCategory).stream().collect(Collectors.groupingBy(ValidationRule::getValidationRuleType));
	}
	
	public List<ValidationRule> findAllValidationRulesBy(String bu, String platform, String dataCategory) {
		var response = webClient.post()
				.uri(dataStudioConfiguration.getValidationRuleURI())
				.body(BodyInserters
						.fromFormData(BU, bu)
						.with(PLATFORM, platform)
						.with(DATA_CATEGORY, dataCategory))
				.retrieve()
				.bodyToMono(new ParameterizedTypeReference<DSResponseDTO<List<ValidationRule>>>() {})
				.block();
		return response.getData();
	}
	
	public Map<UUID,ValidationRule> findAllValidationRulesMapByRuleType(String bu, String platform, String dataCategory, String ruleType) {
		return findAllValidationRulesByRuleType(bu, platform, dataCategory, ruleType).stream().collect(Collectors.toMap(item -> UUID.fromString(item.getSourceColumn()) , Function.identity()));
	}
	
	public List<ValidationRule> findAllValidationRulesByRuleType(String bu, String platform, String dataCategory, String ruleType) {
		var response = webClient.post()
				.uri(dataStudioConfiguration.getValidationRuleByTypeURI())
				.body(BodyInserters
						.fromFormData(BU, bu)
						.with(PLATFORM, platform)
						.with(DATA_CATEGORY, dataCategory)
						.with(RULE_TYPE, ruleType))
				.retrieve()
				.bodyToMono(new ParameterizedTypeReference<DSResponseDTO<List<ValidationRule>>>() {})
				.block();
		return response.getData();
	}
	
	
	public Map<UUID,TransformationRule> findAllTransformationRulesMapBy(String bu, String platform, String dataCategory){
		return findAllTransformationRulesBy(bu, platform, dataCategory).stream().collect(Collectors.toMap(item -> UUID.fromString(item.getSourceColumnName()) , Function.identity()));
	}
	
	public List<TransformationRule> findAllTransformationRulesBy(String bu, String platform, String dataCategory) {
		var response = webClient.post()
				.uri(dataStudioConfiguration.getTransformationRuleURI())
				.body(BodyInserters
						.fromFormData(BU, bu)
						.with(PLATFORM, platform)
						.with(DATA_CATEGORY, dataCategory))
				.retrieve()
				.bodyToMono(new ParameterizedTypeReference<DSResponseDTO<List<TransformationRule>>>() {})
				.block();
		return response.getData();
	}
	
	
	public ColumnConfiguration findProcessConfigurationBy(String bu, String platform, String dataCategory) {
		var response = webClient.post()
				.uri(dataStudioConfiguration.getColumnConfigurationURI())
				.body(BodyInserters
						.fromFormData(BU, bu)
						.with(PLATFORM, platform)
						.with(DATA_CATEGORY, dataCategory))
				.retrieve()
				.bodyToMono(new ParameterizedTypeReference<DSResponseDTO<ColumnConfiguration>>() {})
				.block();
		return response.getData();
	}
	
	public Map<UUID, TargetDataFormat> findTargetDataFormatBy(String bu, String platform, String dataCategory, String dataSetId) {
		var response =  webClient.post()
                .uri(dataStudioConfiguration.getTargetDataFormatURI())
                .body(BodyInserters
                        .fromFormData(BU, bu)
                        .with(PLATFORM, platform)
                        .with(DATA_CATEGORY, dataCategory)
                        .with(DATASET_ID, dataSetId))                
                .retrieve()
                .bodyToMono(new ParameterizedTypeReference<DSResponseDTO<Map<UUID, TargetDataFormat>>>() {})
                .block();
                
        return Objects.nonNull(response.getData()) ? response.getData() : new HashMap<UUID, TargetDataFormat>();       
	}
	
	public void validateRequest(String bu, String platform, String dataCategory) throws ConfigurationException {
		webClient.post()
			.uri(dataStudioConfiguration.getConfigurationValidationURI())
			.body(BodyInserters
					.fromFormData(BU, bu)
					.with(PLATFORM, platform)
					.with(DATA_CATEGORY, dataCategory))
			.retrieve()
			.onStatus(HttpStatusCode::is2xxSuccessful, response -> Mono.empty())
			.onStatus(HttpStatusCode::is4xxClientError, response -> response.bodyToMono(String.class).flatMap(jsonString -> {
				var error = Objects.nonNull(jsonString) ? createConfigurationException(jsonString) : null;
				return Objects.nonNull(error) ? error : Mono.error(new ConfigurationException("Bad Request"));
			}))
			.onStatus(HttpStatusCode::is5xxServerError, response -> response.bodyToMono(String.class).flatMap(jsonString -> {
				var error = Objects.nonNull(jsonString) ? createConfigurationException(jsonString) : null;
				return Objects.nonNull(error) ? error : Mono.error(new ConfigurationException("Internal Server Error"));
			})).toBodilessEntity().block();
		
	}

	private Mono<Throwable> createConfigurationException(String response) {
		var responseJson = new JSONObject(response);
		var error = responseJson.optJSONObject("error");
		if(Objects.nonNull(error)) {
			var errorMessage = error.optString("message",null);
			var exception = new ConfigurationException(errorMessage);
			var errorsJsonArray = error.optJSONArray("errors");
			if (Objects.nonNull(errorsJsonArray)) {
				var errors = IntStream.range(0, errorsJsonArray.length()).mapToObj(errorsJsonArray::getJSONObject)
						.map(errorDataJson -> new ErrorData(errorDataJson.optString("code"), errorDataJson.optString("message"))).collect(Collectors.toList());
				exception.setErrors(errors);
			}
			return Mono.error(exception);
		}
		return null;
	}
}
