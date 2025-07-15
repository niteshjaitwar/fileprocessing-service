package com.adp.esi.digitech.file.processing.generator.document.config.model;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class Shape extends Component {
	
	private String shapeType;
//	private Element element;
	private SConfig config;
}
