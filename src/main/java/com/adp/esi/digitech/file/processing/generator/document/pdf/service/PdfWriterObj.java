package com.adp.esi.digitech.file.processing.generator.document.pdf.service;

import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

import com.lowagie.text.pdf.PdfWriter;

import jakarta.annotation.PreDestroy;
import lombok.Data;

@Component
//@RequestScope
@Data
public class PdfWriterObj {
//	private PdfWriter pdfwriter;
	
	private  final ThreadLocal<PdfWriter> writer=new ThreadLocal<>();
	
	public void setWriter(PdfWriter pdfWriter) {
		writer.set(pdfWriter);
	}
	
	public  PdfWriter getWriter() {
		return writer.get();
	}
	
	public  void clean() {
		writer.remove();
	}
	
	@PreDestroy
	public void cleanUp() {
		clean();
	}
	
}
