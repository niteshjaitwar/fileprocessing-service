package com.adp.esi.digitech.file.processing.ds.config.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ColumnMetaData {

	private String column;
	private char lineIdentifier;
	
	private String prefix;
	private String suffix;
	
	
	private int startPosition;
	private int endPosition;
	
	private String code;
	private String codeValue;
}
