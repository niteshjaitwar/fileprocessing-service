package com.adp.esi.digitech.file.processing.generator.service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.context.WebApplicationContext;

import com.adp.esi.digitech.file.processing.exception.GenerationException;
import com.adp.esi.digitech.file.processing.generator.document.config.model.DVTSDocument;
import com.adp.esi.digitech.file.processing.generator.document.pdf.service.FooterObject;
import com.adp.esi.digitech.file.processing.generator.document.pdf.service.HeaderObject;
import com.adp.esi.digitech.file.processing.generator.document.pdf.service.NewPageEventService;
import com.adp.esi.digitech.file.processing.generator.document.pdf.service.PdfDocumentBuilderImpl;
import com.adp.esi.digitech.file.processing.generator.document.pdf.service.PdfWriterObj;
import com.adp.esi.digitech.file.processing.model.DataSet;
import com.adp.esi.digitech.file.processing.model.Row;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lowagie.text.pdf.PdfDocument;
import com.lowagie.text.pdf.PdfWriter;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class PdfGeneratorService extends AbstractGeneratorService<Row, byte[]> {

	@Autowired
	WebApplicationContext context;
	
	@Autowired
	ObjectMapper objectMapper;

	@Autowired
	PdfWriterObj writerObj;
	
	@Autowired
	NewPageEventService pageEvent;
	
	
    @Override
	public byte[] generate(JSONObject outputFileRule, Map<String, DataSet<Row>> data) throws GenerationException {
		try {
			var document = objectMapper.readValue(outputFileRule.toString(), DVTSDocument.class);
			return generate(document, data);
		} catch (Exception e) {
			log.info("PdfGeneratorService -> generate() Failed Pdf Generation, uniqueId = {}, error = {}", requestContext.getUniqueId(), e.getMessage());
			e.printStackTrace();
			var generationException = new GenerationException("Failed at Pdf Generation", e);
			generationException.setRequestContext(requestContext);
			throw generationException;
		}
	}
	
	
	public byte[] generate(DVTSDocument Pdfdocument, Map<String, DataSet<Row>> data) throws IOException{
		try (ByteArrayOutputStream byteArrayoutputStream = new ByteArrayOutputStream()) {		
			var builder = context.getBean(PdfDocumentBuilderImpl.class);
			var document = builder.newInstance();
			log.info("PDF generation, document instance created and metadata, uniqueId:{}", requestContext.getUniqueId());
			PdfWriter writer=PdfWriter.getInstance(document, byteArrayoutputStream);
			writer.setPageEvent(pageEvent);
//			writerObj.setPdfwriter(writer);
			writerObj.setWriter(writer);
			builder.setMetaData(document, Pdfdocument.getMetaData());
			log.info("PDF generation, Metadata set to document, uniqueId:{}", requestContext.getUniqueId());
			builder.setPages(document, Pdfdocument.getPages(), data);
			log.info("PDF generation, pages created, uniqueId:{}", requestContext.getUniqueId());
			builder.closeInstance(document);
//			header.clean();
//			footer.clean();
//			writerObj.clean();
			return byteArrayoutputStream.toByteArray();
		}
	}
}
