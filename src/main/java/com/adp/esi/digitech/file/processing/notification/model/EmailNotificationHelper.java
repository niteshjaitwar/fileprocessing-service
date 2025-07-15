package com.adp.esi.digitech.file.processing.notification.model;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import lombok.Data;

@Component
@Data
public class EmailNotificationHelper {
	
	@Value("${fileprocessing.mail.to}")
	private String to;
	
	@Value("${fileprocessing.mail.from}")
	private String from;
	
	@Value("${fileprocessing.mail.subject}")
	private String subject;
	
	@Value("${fileprocessing.mail.body}")
	private String body;
	
	@Value("${fileprocessing.mail.mailbodysignature}")
	private String signature;

}
