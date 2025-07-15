package com.adp.esi.digitech.file.processing.generator.document.config.model;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class HDConfig extends Config {

	private String bulletType;
	private String symbol;
	private int topLineSpace;
	private int bottomLineSpace;
	private int internalLineSpace;
	private Font preCharFont;
//	private float leftIndent;
//	private float rightIndent;
}
