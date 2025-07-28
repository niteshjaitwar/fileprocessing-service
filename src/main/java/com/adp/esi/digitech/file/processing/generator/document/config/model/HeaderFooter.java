package com.adp.esi.digitech.file.processing.generator.document.config.model;

import lombok.Data;

@Data
public class HeaderFooter {
	
	private Image image;
	private PageNo pageNo;
	private Content content; 
	private SConfig config;
}
