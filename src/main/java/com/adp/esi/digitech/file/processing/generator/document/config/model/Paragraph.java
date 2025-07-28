package com.adp.esi.digitech.file.processing.generator.document.config.model;

import java.util.List;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class Paragraph extends Component{
	
	private String content;
	private Config config;
	private List<Chunk> chunks;
}
