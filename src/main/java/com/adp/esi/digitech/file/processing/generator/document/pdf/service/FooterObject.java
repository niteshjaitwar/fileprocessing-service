package com.adp.esi.digitech.file.processing.generator.document.pdf.service;

import org.springframework.stereotype.Component;

import com.adp.esi.digitech.file.processing.generator.document.config.model.HeaderFooter;

import jakarta.annotation.PreDestroy;

@Component
public class FooterObject {
private  final ThreadLocal<HeaderFooter> footer=new ThreadLocal<>();
	
	public void setFooter(HeaderFooter hf) {
		footer.set(hf);
	}
	
	public  HeaderFooter getFooter() {
		return footer.get();
	}
	
	public  void clean() {
		footer.remove();
	}
	
	@PreDestroy
	public void cleanUp() {
		clean();
	}
}
