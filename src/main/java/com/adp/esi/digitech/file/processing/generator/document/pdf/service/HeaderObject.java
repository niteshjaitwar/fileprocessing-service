package com.adp.esi.digitech.file.processing.generator.document.pdf.service;

import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

import com.adp.esi.digitech.file.processing.generator.document.config.model.HeaderFooter;
import com.lowagie.text.pdf.PdfWriter;

import jakarta.annotation.PreDestroy;
import lombok.Data;

@Component
//@RequestScope
@Data
public class HeaderObject {
	
//	private HeaderFooter header;
//	private HeaderFooter footer;
	
private  final ThreadLocal<HeaderFooter> header=new ThreadLocal<>();
	
	public void setHeader(HeaderFooter hf) {
		header.set(hf);
	}
	
	public  HeaderFooter getHeader() {
		return header.get();
	}
	
	public  void clean() {
		header.remove();
	}
	
	@PreDestroy
	public void cleanUp() {
		clean();
	}
	
}
