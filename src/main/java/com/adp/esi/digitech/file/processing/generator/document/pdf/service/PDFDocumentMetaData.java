package com.adp.esi.digitech.file.processing.generator.document.pdf.service;

import java.util.Objects;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.adp.esi.digitech.file.processing.generator.document.config.model.MetaData;
import com.adp.esi.digitech.file.processing.util.ValidationUtil;
import com.lowagie.text.Document;

@Component
public class PDFDocumentMetaData {

	@Autowired
	PDFDocumentLayout docLayout;
	
	public void setPDFDocumentMetaData(Document doc, MetaData metaData) {
		if(Objects.nonNull(metaData)) {
			setTitle(doc, metaData.getTitle());
			setAuthor(doc, metaData.getAuthor());
			setSubject(doc, metaData.getSubject());
			setKeywords(doc, metaData.getKeywords());
			if(Objects.nonNull(metaData.getConfig())){
				docLayout.setLayout(doc, metaData.getConfig().getLayout());
			}
		}
	}
	
	private void setTitle(Document doc, String title) {
		if(ValidationUtil.isHavingValue(title))
			doc.addTitle(title);
	}
	
	private void setAuthor(Document doc, String author) {
		if(ValidationUtil.isHavingValue(author))
			doc.addAuthor(author);
	}
	
	private void setSubject(Document doc,String subject) {
		if(ValidationUtil.isHavingValue(subject))
			doc.addSubject(subject);
	}
	
	private void setKeywords(Document doc, String[] keywords) {
		if(Objects.nonNull(keywords) && keywords.length > 0) {
			for(String keyword : keywords) {
				if(ValidationUtil.isHavingValue(keyword))
					doc.addKeywords(keyword);
			}
		}
	}
}
