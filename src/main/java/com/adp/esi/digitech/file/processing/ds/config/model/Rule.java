package com.adp.esi.digitech.file.processing.ds.config.model;

import java.util.List;

import lombok.Data;

@Data
public class Rule {
	
	private String operator;
	private List<Condition> conditions;

}
