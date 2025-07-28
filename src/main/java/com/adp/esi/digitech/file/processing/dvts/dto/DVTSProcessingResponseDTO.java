package com.adp.esi.digitech.file.processing.dvts.dto;

import java.util.List;

import com.adp.esi.digitech.file.processing.enums.Status;
import com.adp.esi.digitech.file.processing.model.Row;
import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.Data;

@Data
public class DVTSProcessingResponseDTO {
	
	@JsonIgnore
	private String dataSetId;
	
	@JsonIgnore
	private String dataSetName;
	
	private Status status;
	
	private List<Row> data;
	
	private String message;
}
