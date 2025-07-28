package com.adp.esi.digitech.file.processing.generator.document.config.model;

import java.util.List;

import lombok.Data;

@Data
public class Page {

	private String dataSetName;
	private HeaderFooter header;
	private HeaderFooter footer;
	private List<Element> elements;
	private List<DataSetMapping> mappings;
}
