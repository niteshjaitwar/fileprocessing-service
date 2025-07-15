package com.adp.esi.digitech.file.processing.generator.document.config.model;

import lombok.Data;

@Data
public class Chunk {
	
	private String name;
	private String content;
	private Config config;
}
