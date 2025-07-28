package com.adp.esi.digitech.file.processing.generator.document.pdf.service;

import java.util.Objects;

import org.springframework.stereotype.Component;

import com.adp.esi.digitech.file.processing.generator.document.config.model.Layout;
import com.lowagie.text.Document;
import com.lowagie.text.PageSize;

@Component
public class PDFDocumentLayout {

	public void setLayout(Document doc, Layout layout) {
		if(Objects.nonNull(layout)) {
			setPageSize(doc,layout.getSize());
			setMarigin(doc,layout);
		}
	}
	
	private void setPageSize(Document doc, String pageSize) {
//		if()
		doc.setPageSize(PageSize.getRectangle(pageSize));
	}
	
	private void setMarigin(Document doc, Layout layout) {
		doc.setMargins(layout.getMariginLeft(), layout.getMariginRight(), layout.getMariginTop(), layout.getMariginBottom());
	}
	
}
