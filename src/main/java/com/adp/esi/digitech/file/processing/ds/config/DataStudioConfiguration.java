package com.adp.esi.digitech.file.processing.ds.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Data;

@Configuration
@ConfigurationProperties(prefix = "digitech.datastudio")
@Data
public class DataStudioConfiguration {
	
	private String lovURI;
	private String configurationURI;
	private String configurationValidationURI;
	private String columnRelationURI;
	private String validationRuleURI;
	private String validationRuleByTypeURI;
	private String transformationRuleURI;
	private String columnConfigurationURI;
	private String targetDataFormatURI;

}
