package com.adp.esi.digitech.file.processing.generator.document.pdf.element.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.adp.esi.digitech.file.processing.generator.document.config.model.Element;
import com.adp.esi.digitech.file.processing.generator.document.config.model.Image;
import com.adp.esi.digitech.file.processing.generator.document.element.IElementBuilder;
import com.adp.esi.digitech.file.processing.generator.document.pdf.service.ImageRetrievalService;
import com.adp.esi.digitech.file.processing.generator.pdf.util.ImageUtils;
import com.adp.esi.digitech.file.processing.model.DataSet;
import com.adp.esi.digitech.file.processing.model.Row;
import com.lowagie.text.Document;

@Component("pdfimages")
public class PDFImageBuilder implements IElementBuilder<Document>{

	@Autowired
	ImageRetrievalService retImgSvc;
	
	@Override
	public void build(Document doc, Element element, DataSet<Row> data) {
		doc.add(getElement(element, data));
	}
	
	@Override
	public com.lowagie.text.Element getElement(Element element, DataSet<Row> data) {
		var image =(Image) element.getComponent();
		return ImageUtils.getImageElement(retImgSvc.getImageFromSource( image.getName(),image.getLocation(),image.getSource()), image.getConfig());
	}
}
