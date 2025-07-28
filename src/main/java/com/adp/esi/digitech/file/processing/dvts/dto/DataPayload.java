package com.adp.esi.digitech.file.processing.dvts.dto;

import java.util.List;
import java.util.Map;

import com.adp.esi.digitech.file.processing.ds.config.model.DataSetRules;
import com.adp.esi.digitech.file.processing.enums.ValidationType;
import com.adp.esi.digitech.file.processing.model.DataMap;
import com.adp.esi.digitech.file.processing.model.RequestContext;

import lombok.Data;

@Data
public class DataPayload {	
	
	private String datasetId;
	
	private String datasetName;
	
	private int batchSize;
	
	private String batchName;
	
	private ValidationType validationType;
	
	private RequestContext requestContext;
	
	private List<DataMap> data;
	
	private Map<String, String> clause;
	
	private DataSetRules dataSetRule;
	
	private List<String> columnsToValidate;
}
