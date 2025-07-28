package com.adp.esi.digitech.file.processing.generator.document.pdf.service;

import java.util.Base64;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;


@Component
public class ImageRetrievalService {

	@Autowired
	Environment env;
	
	public byte[] fetchImageFromAppProps(String property) {
		byte[] imageData=null;
		String b64String=env.getProperty(property);
		try {
//			Base64.getDecoder().decode(property);
			imageData=Base64.getDecoder().decode(b64String);
		}catch(Exception e) {
			throw new IllegalArgumentException("Not valid Image", e);
		}
		return imageData;
	}
	
	public byte[] getImageFromSource(String name, String location, String source) {
		switch(source) {
		case "application.properties":
			return fetchImageFromAppProps(name);
		default:
			return null;
			
		}
	}
	
}
