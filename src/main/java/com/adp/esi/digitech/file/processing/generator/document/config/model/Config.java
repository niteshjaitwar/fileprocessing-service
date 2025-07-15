package com.adp.esi.digitech.file.processing.generator.document.config.model;

import lombok.Data;

@Data
public class Config {
	
	private float width;
	private float height;
	private String alignment;
	private Font font;
	private String borderColor;
	private float borderDensity;
	private float lineSpacing;
	private float indent;
	
}
