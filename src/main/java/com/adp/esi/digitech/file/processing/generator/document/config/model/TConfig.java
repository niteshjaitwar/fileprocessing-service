package com.adp.esi.digitech.file.processing.generator.document.config.model;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class TConfig extends Config {

	private String bgColor;
	private int colspan;
	private String isTableTranspose;
	private int maxRows;
	private float padding;
}
