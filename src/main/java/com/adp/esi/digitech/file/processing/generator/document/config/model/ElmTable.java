package com.adp.esi.digitech.file.processing.generator.document.config.model;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class ElmTable extends Component {

	private THeader header;
	private TBody body;
	private TConfig config;
}
