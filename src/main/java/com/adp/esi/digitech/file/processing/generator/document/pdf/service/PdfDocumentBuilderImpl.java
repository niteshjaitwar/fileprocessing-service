package com.adp.esi.digitech.file.processing.generator.document.pdf.service;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.context.WebApplicationContext;

import com.adp.esi.digitech.file.processing.generator.document.AbstarctDocumentBuilder;
import com.adp.esi.digitech.file.processing.generator.document.config.model.DataSetMapping;
import com.adp.esi.digitech.file.processing.generator.document.config.model.MetaData;
import com.adp.esi.digitech.file.processing.generator.document.config.model.Page;
import com.adp.esi.digitech.file.processing.model.DataSet;
import com.adp.esi.digitech.file.processing.model.RequestContext;
import com.adp.esi.digitech.file.processing.model.Row;
import com.lowagie.text.Document;

import lombok.extern.slf4j.Slf4j;

@Service
public class PdfDocumentBuilderImpl extends AbstarctDocumentBuilder<Document>{

	@Autowired
	WebApplicationContext context;
	
	@Autowired
	PDFDocumentMetaData metaData;
	
//	RequestContext requestContext;
	
	@Override
	public Document newInstance() {
		Document doc = new Document();
		return doc;
	}
	
	public void setMetaData(Document doc,MetaData metaDataConfig ) {
		metaData.setPDFDocumentMetaData(doc, metaDataConfig);
	}

	@Override
	public void setPages(Document doc, List<Page> pageConfigs, Map<String, DataSet<Row>> dataSets) {
		var pageBuilder = context.getBean(PdfPageBuilderImpl.class);
		if(Objects.nonNull(pageConfigs)) {
			pageConfigs.stream().forEach(pageConfig -> {
				if(Objects.nonNull(pageConfig)) {
					pageBuilder.newInstance(doc, pageConfig);
					pageBuilder.setElements(doc, pageConfig, getDataSet(pageConfig, dataSets));
				}
			});
		}
	}
	
	public void closeInstance(Document document) {
		if(document.isOpen()) {
			document.close();
		}
	}
	
	private DataSet<Row> getDataSet(Page pageConfig,Map<String, DataSet<Row>> dataSets){
		DataSet<Row> dataSet=null;
		if(Objects.nonNull(pageConfig) && Objects.nonNull(pageConfig.getDataSetName()) && !pageConfig.getDataSetName().isBlank()) {
			dataSet=dataSets.get(pageConfig.getDataSetName());
			setColumnIds(dataSet, pageConfig);
			
		}
		return dataSet;
	}
	
	private DataSet<Row> setColumnIds(DataSet<Row> dataSet, Page pageConfig){
		
		if(Objects.nonNull(pageConfig) && Objects.nonNull(pageConfig.getMappings())) {
			List<DataSetMapping> mappings=pageConfig.getMappings();

			for(int j=0;j<dataSet.getData().size();j++) {
				for(int i=0;i<mappings.size(); i++) {
					if(Objects.nonNull(dataSet.getData().get(j).getColumns().get(UUID.fromString(mappings.get(i).getField())))) {
						dataSet.getData().get(j).getColumns().get(UUID.fromString(mappings.get(i).getField())).setName(mappings.get(i).getName());
					}
				}
			}
		}
		
		return dataSet;
	}
	
}
