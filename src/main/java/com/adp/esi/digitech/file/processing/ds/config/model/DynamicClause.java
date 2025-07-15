package com.adp.esi.digitech.file.processing.ds.config.model;

import lombok.Data;

@Data
public class DynamicClause {
	
	private String name;
	private String column;
	private String operator;
	private String level;
	private String value;
}
