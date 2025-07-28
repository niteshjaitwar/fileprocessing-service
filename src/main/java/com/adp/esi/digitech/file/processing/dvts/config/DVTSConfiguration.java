package com.adp.esi.digitech.file.processing.dvts.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Data;

@Configuration
@ConfigurationProperties(prefix = "digitech.dvts")
@Data
public class DVTSConfiguration {
	
	private String validationURI;
	private String transformationURI;
	private String processURI;

}
