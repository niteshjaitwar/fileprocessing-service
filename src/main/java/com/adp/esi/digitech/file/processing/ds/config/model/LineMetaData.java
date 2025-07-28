package com.adp.esi.digitech.file.processing.ds.config.model;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class LineMetaData {
	
	private String lineName;
	private String groupIdentifier;
	private ColumnMetaData[] columnMetaData;	
	
	@JsonIgnore
	private List<String[]> lineIdentifiers;	
	
	@JsonIgnore
	private Map<String, Integer[]> columnIdentifierPositions;	
	
	@JsonIgnore
	private int codeStartIndex;
	
	@JsonIgnore
	private Map<String, String[]> codeIdentifers;
	
	
}
