package com.adp.esi.digitech.file.processing.generator.document.pdf.service;

import java.util.Objects;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.file.processing.generator.document.AbstractPageBuilder;
import com.adp.esi.digitech.file.processing.generator.document.config.model.HeaderFooter;
import com.adp.esi.digitech.file.processing.generator.document.config.model.Page;
import com.adp.esi.digitech.file.processing.generator.document.pdf.element.service.PDFElementRouterImpl;
import com.adp.esi.digitech.file.processing.generator.pdf.util.HeaderFooterUtils;
import com.adp.esi.digitech.file.processing.model.DataSet;
import com.adp.esi.digitech.file.processing.model.Row;

import com.lowagie.text.Document;


@Service
public class PdfPageBuilderImpl extends AbstractPageBuilder<Document> {

	@Autowired
	PDFElementRouterImpl routerImpl;
	
	@Autowired
	PdfWriterObj writerObj;
	
	@Autowired
	HeaderFooterUtils headerFooterUtils;
	
	@Autowired
	NewPageEventService pageEvent;
	
	@Autowired
	HeaderObject headerObject;
	
	@Autowired
	FooterObject footerObject;
	
	
	@Override
	public void newInstance(Document doc, Page page) {
		headerObject.setHeader(page.getHeader());
		footerObject.setFooter(page.getFooter());
		newPage(doc);//create page event and so header and footer are first pushed
	}
	
	
	@Override
	public void setHeaderFooter(Document doc, HeaderFooter header, HeaderFooter footer) {
//		headerFooterUtils.createHeaderFooter(writerObj.getPdfwriter(), doc, header, footer);
		headerFooterUtils.createHeaderFooter(writerObj.getWriter(), doc, header, footer);
	}
	
	


	@Override
	public void setElements(Document doc, Page page, DataSet<Row> data) {
		if(Objects.nonNull(page.getElements()) && page.getElements().size()>0)
		page.getElements().stream().forEach(elementConfig -> {
			routerImpl.route(doc, elementConfig, data);
		});
	}

	
	private void newPage(Document doc) {
		if(doc.isOpen()) {
			doc.newPage();
		}else {
			doc.open();
		}
	}

}
