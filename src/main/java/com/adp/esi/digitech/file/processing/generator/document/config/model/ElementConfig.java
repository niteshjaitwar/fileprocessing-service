package com.adp.esi.digitech.file.processing.generator.document.config.model;

import lombok.Data;

@Data
public class ElementConfig <T extends Element> {

	private String type;
	private T element;
}
