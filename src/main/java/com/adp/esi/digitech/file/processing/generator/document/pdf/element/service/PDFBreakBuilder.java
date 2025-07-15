package com.adp.esi.digitech.file.processing.generator.document.pdf.element.service;

import org.springframework.stereotype.Component;

import com.adp.esi.digitech.file.processing.generator.document.config.model.Break;
import com.adp.esi.digitech.file.processing.generator.document.config.model.Element;
import com.adp.esi.digitech.file.processing.generator.document.element.IElementBuilder;
import com.adp.esi.digitech.file.processing.generator.document.enums.BreakType;
import com.adp.esi.digitech.file.processing.generator.pdf.util.GenericUtils;
import com.adp.esi.digitech.file.processing.model.DataSet;
import com.adp.esi.digitech.file.processing.model.Row;
import com.lowagie.text.Document;

@Component("pdfbreak")
public class PDFBreakBuilder implements IElementBuilder<Document>{

	@Override
	public void build(Document doc, Element elementConfig, DataSet<Row> data) {
		
		var breakElement = (Break) elementConfig.getComponent();
		createBreak(doc, breakElement);
	}
	
	@Override
	public com.lowagie.text.Element getElement(Element element, DataSet<Row> data) {
		// This element cannot be retrieved
		return null;
	}
	
	public void createBreak(Document doc,Break breakElement) {
		switch(BreakType.valueOf(breakElement.getBreakType())) {
		case Page:
			GenericUtils.createPageSpace(doc);
			break;
		
		case Space:
			GenericUtils.createMultipleLinesSpace(doc, breakElement.getLines());
			break;
		
		default:
			GenericUtils.createMultipleLinesSpace(doc, breakElement.getLines());
			break;
		}
	}



}
