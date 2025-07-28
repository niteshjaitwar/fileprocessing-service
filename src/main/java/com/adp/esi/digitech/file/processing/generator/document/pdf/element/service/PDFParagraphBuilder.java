package com.adp.esi.digitech.file.processing.generator.document.pdf.element.service;

import org.springframework.stereotype.Component;

import com.adp.esi.digitech.file.processing.generator.document.config.model.Element;
import com.adp.esi.digitech.file.processing.generator.document.config.model.Paragraph;
import com.adp.esi.digitech.file.processing.generator.document.element.IElementBuilder;
import com.adp.esi.digitech.file.processing.generator.pdf.util.ParagraphUtils;
import com.adp.esi.digitech.file.processing.model.DataSet;
import com.adp.esi.digitech.file.processing.model.Row;
import com.lowagie.text.Document;

@Component("pdfparagraph")
public class PDFParagraphBuilder implements IElementBuilder<Document>{

	@Override
	public void build(Document doc, Element element, DataSet<Row> data) {
		doc.add(getElement(element,data));
	}

	@Override
	public com.lowagie.text.Element getElement(Element element, DataSet<Row> data) {
		var paragraph = (Paragraph) element.getComponent();
		return ParagraphUtils.createParagraph(paragraph,data);
	}

	
}
