package com.adp.esi.digitech.file.processing.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration
public class WebClientConfiguration {
	
	@Value("${digitech.datastudio.server.url}")
	private String dataStudioUrl;
	
	@Value("${digitech.dvts.server.url}")
	private String dvtsServerUrl;
	
	@Value("${autojobs.rest.baseUrl}")
	private String autoJobsServerUrl;
	
	@Value("${autojobs.rest.credentials.username}")
	private String autoJobsUsername;
	
	@Value("${autojobs.rest.credentials.password}")
	private String autoJobspassword;	
	
	@Value("${digitech.webclient.max.inMemory.buffer.size}")
	private int bufferSize;
	
	
	@Bean(name = "dataStudioWebClient")	
	public WebClient dataStudioWebClient() {
		return WebClient.builder()
				.baseUrl(dataStudioUrl)
				.defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.MULTIPART_FORM_DATA_VALUE)
				.codecs(codecs -> codecs
						.defaultCodecs()
						.maxInMemorySize(1024 * 1024 * bufferSize))
				.build();
	}
	
	@Bean(name = "dvtsWebClient")	
	public WebClient dvtsWebClient() {
		return WebClient.builder()
				.baseUrl(dvtsServerUrl)
				.defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
				.defaultHeader(HttpHeaders.CONTENT_ENCODING, "gzip")
				.codecs(codecs -> codecs
						.defaultCodecs()
						.maxInMemorySize(1024 * 1024 * bufferSize))
				.build();
	}
	
	@Bean(name = "autoJobsWebClient")	
	public WebClient autoJobsWebClient() {
		return WebClient.builder()
				.baseUrl(autoJobsServerUrl)
				.defaultHeaders(headers -> {
					headers.setBasicAuth(autoJobsUsername, autoJobspassword);
					headers.setContentType(MediaType.APPLICATION_JSON);
                })
				.codecs(codecs -> codecs
						.defaultCodecs()
						.maxInMemorySize(1024 * 1024 * bufferSize))
				.build();
	}

}
