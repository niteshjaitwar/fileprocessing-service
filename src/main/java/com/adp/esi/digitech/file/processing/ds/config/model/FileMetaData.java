package com.adp.esi.digitech.file.processing.ds.config.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class FileMetaData {
	private String sourceKey;
	private String type;
	private String delimeter;
	private String processing;
	private String processingType;
	private String filterData;
	private int headerIndex;
	private String primaryIdentifier;
	
	@JsonProperty("static")
	private String[][] staticLines;
	private String[] headers;
	private LineMetaData[] lines;
	private String[][] trailer;
	
	private String template;
	
	//Large File Attributes
	private String groupIdentifier;
	private int batchSize;
	
	private int txtLinesCount;
	
	public FileMetaData(String sourceKey, String type, String delimeter, String processing, int headerIndex) {
		super();
		this.sourceKey = sourceKey;
		this.type = type;
		this.delimeter = delimeter;
		this.processing = processing;
		this.headerIndex = headerIndex;
	}
	
	
}
