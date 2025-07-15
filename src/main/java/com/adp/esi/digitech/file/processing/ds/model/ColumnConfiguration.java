package com.adp.esi.digitech.file.processing.ds.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class ColumnConfiguration {
	
	@JsonProperty("configuration-data")
	ConfigurationData configurationData;
	
	@JsonProperty("column-relations")
	List<ColumnRelation> columnRelations;
	
	@JsonProperty("validation-rules")
	List<ValidationRule> validationRules;
	
	@JsonProperty("transformation-rules")
	List<TransformationRule> transformationRules;
	
}
