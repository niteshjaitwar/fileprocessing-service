package com.adp.esi.digitech.file.processing.reader.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.adp.esi.digitech.file.processing.ds.config.model.FileMetaData;
import com.adp.esi.digitech.file.processing.ds.model.ColumnRelation;
import com.adp.esi.digitech.file.processing.exception.ReaderException;
import com.adp.esi.digitech.file.processing.model.DataMap;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

@Service("csvReaderService")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class CSVReaderService extends AbstractCSVReaderService<DataMap, MultipartFile> {
	
	private List<ColumnRelation> columnRelations;
	private FileMetaData fileMetaData;
	
	
	@Autowired(required = true)
	public CSVReaderService(FileMetaData fileMetaData, List<ColumnRelation> columnRelations) {
		this.fileMetaData = fileMetaData;
		this.columnRelations = columnRelations;		
	}

	@Override
	public Map<String, List<DataMap>> read(MultipartFile file) throws ReaderException {
		log.info("CSVReaderService -> read() Started processing csv, uniqueId = {}, sourceKey = {}", requestContext.getUniqueId(), fileMetaData.getSourceKey());
	
		try(var inputStreamReader = new InputStreamReader(file.getInputStream(), StandardCharsets.ISO_8859_1);
			var reader = new BufferedReader(inputStreamReader)) {
			
			var format = this.newCsvFormat(fileMetaData, reader);
			
			@Cleanup var parser = CSVParser.parse(reader, format);
			
			if(HEADER.equalsIgnoreCase(fileMetaData.getProcessing())) {
				this.validate(fileMetaData, columnRelations, parser.getHeaderNames());
			}
			
			var rows = parser.stream().map(record -> {	
				if(!HEADER.equalsIgnoreCase(fileMetaData.getProcessing()) && record.getRecordNumber() == 1) {
					this.validate(fileMetaData, columnRelations, record);
				}
				var columnMap = columnRelations.parallelStream()
								.collect(HashMap<UUID,String>::new, 
								(map,columnRelation) -> map.put(UUID.fromString(columnRelation.getUuid()), getValue(record, columnRelation, fileMetaData.getProcessing())), 
								HashMap<UUID,String>::putAll);						
				
				return new DataMap(columnMap);				
			}).collect(Collectors.toList());
			
			//var data = new HashMap<String, List<DataMap>>();			
			//data.put(fileMetaData.getSourceKey(), rows);
			
			log.info("CSVReaderService -> read() Completed processing csv, uniqueId = {}, sourceKey = {}", requestContext.getUniqueId(), fileMetaData.getSourceKey());			
			return Map.of(fileMetaData.getSourceKey(), rows);
			
		} catch (IOException e) {
			log.error("CSVReaderService -> read() Failed to processing csv, uniqueId = {}, sourceKey = {}, message = {}",	requestContext.getUniqueId(), fileMetaData.getSourceKey(), e.getMessage());
			var readerException = new ReaderException("CSV Parsing failed, reason = " + e.getMessage(), e.getCause());
			readerException.setRequestContext(requestContext);
			throw readerException;
		}
	}
}
