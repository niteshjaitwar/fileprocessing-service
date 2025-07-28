package com.adp.esi.digitech.file.processing.generator.document.config.model;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
public class DVTSDocument {

	private String outputFileType;
	private String dataSetName;
	private FileName fileName;
	private MetaData metaData;
	private List<Page> pages;
	private String documentId;
}
