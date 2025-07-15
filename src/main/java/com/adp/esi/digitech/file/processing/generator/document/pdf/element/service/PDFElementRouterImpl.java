package com.adp.esi.digitech.file.processing.generator.document.pdf.element.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.context.WebApplicationContext;

import com.adp.esi.digitech.file.processing.generator.document.config.model.Element;
import com.adp.esi.digitech.file.processing.generator.document.element.IElementBuilder;
import com.adp.esi.digitech.file.processing.generator.document.element.IElementRouter;
import com.adp.esi.digitech.file.processing.generator.document.enums.ElementType;
import com.adp.esi.digitech.file.processing.model.DataSet;
import com.adp.esi.digitech.file.processing.model.Row;
import com.lowagie.text.Document;

@Service
public class PDFElementRouterImpl implements IElementRouter<Document>{

	@Autowired
	private WebApplicationContext context;
	
	@Override
	public void route(Document doc, Element element, DataSet<Row> data) {
		
		String type = element.getType();
		IElementBuilder<Document> elementBuilder = getElementBean(type);
		elementBuilder.build(doc, element, data);
	}
	
	public com.lowagie.text.Element getElement(Element element, DataSet<Row> data){
		String type = element.getType();
		IElementBuilder<Document> elementBuilder = getElementBean(type);
		com.lowagie.text.Element pdfElement=elementBuilder.getElement(element, data);
		return pdfElement;
	}
	
	private IElementBuilder<Document> getElementBean(String type) {
		IElementBuilder<Document> elementBuilder = null;
		switch (ElementType.valueOf(type)) {
		case Paragraph:
			elementBuilder = context.getBean(PDFParagraphBuilder.class);
			break;
		case Table:
			elementBuilder = context.getBean(PDFTableBuilder.class);
			break;
		case Image:
			elementBuilder = context.getBean(PDFImageBuilder.class);
			break;
		case Shape:
			elementBuilder = context.getBean(PDFShapeBuilder.class);
			break;
		case HDList:
			elementBuilder = context.getBean(PDFHDListBuilder.class);
			break;
		case Break:
			elementBuilder = context.getBean(PDFBreakBuilder.class);
			break;				
		default:
			throw new IllegalArgumentException("Unexpected value: " + ElementType.valueOf(type));
		}
		return elementBuilder;
	}

}
