package com.adp.esi.digitech.file.processing.model;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.adp.esi.digitech.file.processing.ds.config.model.TargetDataFormat;

import lombok.Data;

@Data
public class DataSet<T> {
	private String id;
	
	private String name;	
	
	private List<T> data;
	
	private List<String> columnsToValidate;
	
	private Map<UUID, TargetDataFormat> targetFormatMap;
	
	private int batchSize;
	
	private String batchName;
	
}
	//private List<Row> transformedData;
	//private String bu; 

	//private String platform; 
	
	//private String dataCategory; 
	
	//private String subDataCategory;
	
	//private String uniqueId;
	
	//private String saveFileLocation;

	//private Map<UUID, ValidationRule> validationRules;

	//private Map<UUID, ValidationRule> camValidationRules;
	
	///private Map<UUID, TransformationRule> transformationRules;
	
	//private Map<String, Properties> lovMetadataMap;