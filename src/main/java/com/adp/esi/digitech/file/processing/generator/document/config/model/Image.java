package com.adp.esi.digitech.file.processing.generator.document.config.model;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class Image extends Component {

	private String name;
	private String location;
	private String source;
	private Config config;
}
