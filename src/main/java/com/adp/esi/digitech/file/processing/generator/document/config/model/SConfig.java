package com.adp.esi.digitech.file.processing.generator.document.config.model;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class SConfig extends Config {
	
	private float borderDensity;
	private String borderColor;
	private float startXPosition;
	private float startYPosition;
}
