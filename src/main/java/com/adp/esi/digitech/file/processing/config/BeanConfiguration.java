package com.adp.esi.digitech.file.processing.config;

import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.util.UUID;

import javax.xml.stream.XMLOutputFactory;

import org.apache.poi.ss.usermodel.DataFormatter;
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import com.adp.esi.digitech.file.processing.enums.TargetLocation;
import com.adp.esi.digitech.file.processing.model.Column;
import com.adp.esi.digitech.file.processing.model.RequestContext;
import com.adp.esi.digitech.file.processing.file.service.FileService;
import com.adp.esi.digitech.file.processing.file.service.IFileService;
import com.adp.esi.digitech.file.processing.file.service.SharePointFileService;
import com.adp.esi.digitech.file.processing.file.service.SharedDriveFileService;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

@Configuration
public class BeanConfiguration {
	
	@Value("${jasypt.encryptor.algorithm}")
	private String algorithm;
	
	@Value("${jasypt.encryptor.password}")
	private String password;
	
	@Bean
	public ModelMapper modelMapper() {
		return new ModelMapper();
	}
	
	@Bean
	public SimpleDateFormat simpleDateFormat() {
		SimpleDateFormat s = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		s.setTimeZone(TimeZone.getTimeZone("GMT"));
		return s;
	}
	
	@Bean
	public DataFormatter dataFormatter() {
		return new DataFormatter();
	}
	
	@Bean
	public JsonFactory jsonFactory() {
		return  objectMapper().getFactory();
	}
	
	@Bean
	public ObjectMapper objectMapper() {
		var mapper = new ObjectMapper();
		mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
		//mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		//mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
		return mapper;
	}
	
	@Bean
	@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
	public Column column(String name, Object value, UUID uuid, String sourceKey) {
		return new Column(name, value, uuid, sourceKey);
	}
	
	@Bean
	@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
	public IFileService IFilesService(RequestContext requestContext, TargetLocation targetLocation) {
		IFileService iFilesService = null;
		switch (targetLocation) {
		case SharedDrive:
			iFilesService = new SharedDriveFileService();
			iFilesService.setRequestContext(requestContext);
			break;
		case SharePoint:
			iFilesService = new SharePointFileService();
			iFilesService.setRequestContext(requestContext);
			break;
		case Local:
			iFilesService = new FileService();
			iFilesService.setRequestContext(requestContext);
			break;
		default:
			iFilesService = new SharedDriveFileService();
			iFilesService.setRequestContext(requestContext);
			break;
		}
		
		return iFilesService;		
	}
	
	@Bean
	public StandardPBEStringEncryptor standardPBEStringEncryptor() {
		StandardPBEStringEncryptor standardPBEStringEncryptor = new StandardPBEStringEncryptor();
		standardPBEStringEncryptor.setAlgorithm(algorithm);
		standardPBEStringEncryptor.setPassword(password);
		return standardPBEStringEncryptor;
	}
	
	@Bean
	@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
	public XMLOutputFactory xMLOutputFactory() {
		return XMLOutputFactory.newInstance();
	}
}
