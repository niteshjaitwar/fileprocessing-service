package com.adp.esi.digitech.file.processing.ds.config.model;

import lombok.Data;

@Data
public class Condition {
	
	private String column;
	private String dynamic;
	private String value;
	private String operator;

}
