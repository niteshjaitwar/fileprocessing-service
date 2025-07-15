package com.adp.esi.digitech.file.processing.ds.config.model;

import java.util.List;

import lombok.Data;

@Data
public class InputRule {
	private String sourceKey;
	private String type;
	private String dataSetName;
	private String dataSetId;
	private int batchSize;
	private int headerIndex;
	private int sequence;
	private List<Reference> references;
}
