package com.adp.esi.digitech.file.processing.document.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.adp.esi.digitech.file.processing.document.service.PDFDocumentService;
import com.adp.esi.digitech.file.processing.exception.ConfigurationException;
import com.adp.esi.digitech.file.processing.exception.GenerationException;

import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("/ahub/fileprocessing/v2/pdf/")
@CrossOrigin(origins = "${app.allowed-origins}")
@Slf4j
public class PDFDocumentController {
	
	@Autowired
	PDFDocumentService pdfDocumentService;
	
	@PostMapping("/preview")
	public ResponseEntity<Resource> preview(String bu, String platform, String dataCategory, String documentId) throws ConfigurationException, GenerationException {
		log.info("Request received for pdf preview with documentId:{}",documentId);
		var bytes = pdfDocumentService.preview(bu, platform, dataCategory, documentId);
		var headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_PDF);
		headers.setContentDispositionFormData("attachment", documentId.strip() + ".pdf");
		return ResponseEntity.ok().headers(headers).body(new ByteArrayResource(bytes));
	}
	
	@PostMapping("/preview-payload")
	public ResponseEntity<Resource> preview(String bu, String platform, String dataCategory, String documentId, @RequestBody String payload ) throws ConfigurationException, GenerationException {
		var bytes = pdfDocumentService.previewWithPayload(bu, platform, dataCategory, documentId,payload);
		var headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_PDF);
		headers.setContentDispositionFormData("attachment", documentId.strip() + ".pdf");
		return ResponseEntity.ok().headers(headers).body(new ByteArrayResource(bytes));
	}
	
	

}
