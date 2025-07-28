package com.adp.esi.digitech.file.processing.generator.document.pdf.service;

import java.util.Objects;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.adp.esi.digitech.file.processing.generator.pdf.util.HeaderFooterUtils;
import com.lowagie.text.Document;
import com.lowagie.text.pdf.PdfPageEventHelper;
import com.lowagie.text.pdf.PdfWriter;

@Component
public class NewPageEventService extends PdfPageEventHelper{
	
	@Autowired
	HeaderFooterUtils headerFooterUtils;
	
	@Autowired
	HeaderObject headerObject;
	
	@Autowired
	FooterObject FooterObject;
	
	@Override
	public void onStartPage(PdfWriter writer, Document document) {
			headerFooterUtils.createHeaderFooter(writer, document, headerObject.getHeader(), FooterObject.getFooter());
    }
	
}
