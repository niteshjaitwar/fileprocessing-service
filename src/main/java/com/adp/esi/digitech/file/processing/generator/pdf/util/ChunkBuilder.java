package com.adp.esi.digitech.file.processing.generator.pdf.util;

import com.adp.esi.digitech.file.processing.generator.document.config.model.Font;
import com.adp.esi.digitech.file.processing.util.ValidationUtil;
import com.lowagie.text.Chunk;

public class ChunkBuilder {

	public static Chunk create(String content, Font fontConfig) {
		Chunk chunk=new Chunk("");
		if(ValidationUtil.isHavingValue(content)) {
			chunk=new Chunk(content,FontUtils.buildFont(fontConfig));
		}
		return chunk;
	}
}
