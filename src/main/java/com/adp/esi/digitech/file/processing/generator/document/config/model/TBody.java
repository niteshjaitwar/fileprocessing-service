package com.adp.esi.digitech.file.processing.generator.document.config.model;

import lombok.Data;

@Data
public class TBody {

	private String contentType;
	private TRow dynamicRows;
	private TRow[] staticRows;
	private TConfig config;
}
