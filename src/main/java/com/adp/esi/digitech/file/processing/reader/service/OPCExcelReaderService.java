package com.adp.esi.digitech.file.processing.reader.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.openxml4j.opc.PackageAccess;
import org.apache.poi.util.XMLHelper;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.adp.esi.digitech.file.processing.autowire.service.CustomReaderDynamicAutowireService;
import com.adp.esi.digitech.file.processing.ds.config.model.FileMetaData;
import com.adp.esi.digitech.file.processing.ds.model.ColumnRelation;
import com.adp.esi.digitech.file.processing.exception.MetadataValidationException;
import com.adp.esi.digitech.file.processing.exception.ReaderException;
import com.adp.esi.digitech.file.processing.model.ChunkDataMap;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Service
public class OPCExcelReaderService extends AbstractExcelReaderService<ChunkDataMap, String> {

	private String fileName;
	private Map<String, List<ColumnRelation>> columnRelationsMap;
	private Map<String, FileMetaData> fileMetaDataMap;

	@Autowired
	CustomReaderDynamicAutowireService customReaderDynamicAutowireService;
	
	@Autowired(required = true)
	public OPCExcelReaderService(String fileName, Map<String, List<ColumnRelation>> columnRelationsMap, Map<String, FileMetaData> fileMetaDataMap) {
		this.fileName = fileName;
		this.columnRelationsMap = columnRelationsMap;
		this.fileMetaDataMap = fileMetaDataMap;
	}


	@Override
	public Map<String, List<ChunkDataMap>> read(String filePath) throws ReaderException {
		log.info("OPCExcelReaderService -> read() Started processing excel, uniqueId = {}, fileName = {}", requestContext.getUniqueId(), fileName);
		
		try (OPCPackage opcPak = OPCPackage.open(filePath, PackageAccess.READ);){

			XSSFReader reader = new XSSFReader(opcPak);			
			var sst = reader.getSharedStringsTable();
			XSSFReader.SheetIterator itr = (XSSFReader.SheetIterator)reader.getSheetsData();			
			
			List<String> sheetNames = new ArrayList<>();
			var workbookXML = reader.getWorkbookData();			
			var sheetNameHandler = new SheetNameHandler(sheetNames);
			var parser = XMLHelper.newXMLReader();
			parser.setContentHandler(sheetNameHandler);
			parser.parse(new InputSource(workbookXML));			
			
			int noOfSheets = sheetNames.size();
			
			if(fileMetaDataMap.size() == 1 && noOfSheets == 1) {
				try(var sheet = itr.next()){
					//var sheetName = itr.getSheetName();
					var sourceKey = fileName;
					var columnRelations = columnRelationsMap.get(sourceKey);
					var fileMetaData = fileMetaDataMap.get(sourceKey);
					
					var sheetData = customReaderDynamicAutowireService.readLargeSheetData(sheet, fileMetaData, columnRelations, this.requestContext,sst);
					log.info("OPCExcelReaderService -> read() completed processing excel, uniqueId = {}, fileName = {}", requestContext.getUniqueId(), fileName);
					
					return sheetData;
				}
			}
			
			
			//Checking for configured sheets in the excel, If any configured sheet missing in excel we are throwing error
			var invalidFileMetadataList = fileMetaDataMap.entrySet().stream().filter(entry -> {
				var key = entry.getKey();
				var sheetName = key.contains("{{") && key.endsWith("}}") ? key.substring(key.indexOf("{{") + 2, key.indexOf("}}")) : key;
				return !sheetNames.contains(sheetName);
            }).map(entry -> entry.getValue()).collect(Collectors.toList());
			
			
			if(Objects.nonNull(invalidFileMetadataList) && !invalidFileMetadataList.isEmpty()) {
				createMetadataValidationExceptionSheet(invalidFileMetadataList);
			}
			
			Map<String, List<ChunkDataMap>> sourceData = new HashMap<>();
			
			//boolean isSheetOne = true;
			while (itr.hasNext()) {
				try(var sheet = itr.next()){
					var sheetName = itr.getSheetName();
					//if(itr.hasNext())
						//isSheetOne = false;
					//var sourceKey = isSheetOne ? fileName : fileName.concat("{{").concat(sheetName).concat("}}");
					var sourceKey = fileName.concat("{{").concat(sheetName).concat("}}");
					
					if(fileMetaDataMap.containsKey(sourceKey)) {
						var fileMetaData = fileMetaDataMap.get(sourceKey);
						var sourceDataMap = customReaderDynamicAutowireService.readLargeSheetData(sheet, fileMetaData, columnRelationsMap.get(sourceKey), this.requestContext,sst);				
						sourceData.putAll(sourceDataMap);
					}
				}
			}
			log.info("OPCExcelReaderService -> read() completed processing excel, uniqueId = {}, fileName = {}", requestContext.getUniqueId(), fileName);
			return sourceData;
		} catch (MetadataValidationException e) {
			log.error("OPCExcelReaderService -> read() Failed to processing excel with metadata validations, uniqueId = {}, fileName = {}, message = {}", requestContext.getUniqueId(), fileName, e.getMessage());
			throw e;
		}catch (Exception e) {
			log.error("OPCExcelReaderService -> read() Failed to processing excel, uniqueId = {}, fileName = {}, message = {}", requestContext.getUniqueId(), fileName, e.getMessage());
			var readerException = new ReaderException("Excel reading failed, reason = " + e.getMessage(), e.getCause());
			readerException.setRequestContext(requestContext);
			throw readerException;
		}
		
	}
	
	private class SheetNameHandler extends DefaultHandler {
		private final List<String> sheetNames;
		
		private SheetNameHandler(List<String> sheetNames) {
			this.sheetNames = sheetNames;
		}

		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes)
				throws SAXException {
			if("sheet".equals(qName)) {
				var sheetName = attributes.getValue("name");
				sheetNames.add(sheetName);
			}
		}
		
	}
}
