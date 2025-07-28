package com.adp.esi.digitech.file.processing.generator.document.config.model;

import lombok.Data;

@Data
public class MetaData {

	private String title;
	private String author;
	private String subject;
	private String[] keywords;
	private MConfig config;
//	private Layout layout
	
}
