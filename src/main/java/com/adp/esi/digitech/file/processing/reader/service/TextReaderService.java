package com.adp.esi.digitech.file.processing.reader.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.adp.esi.digitech.file.processing.autowire.service.CustomReaderDynamicAutowireService;
import com.adp.esi.digitech.file.processing.ds.config.model.FileMetaData;
import com.adp.esi.digitech.file.processing.ds.model.ColumnRelation;
import com.adp.esi.digitech.file.processing.enums.ReadType;
import com.adp.esi.digitech.file.processing.exception.ReaderException;
import com.adp.esi.digitech.file.processing.model.DataMap;
import com.adp.esi.digitech.file.processing.reader.text.service.TextDelimetedReaderService;
import com.adp.esi.digitech.file.processing.reader.text.service.TextHeaderReaderService;
import com.adp.esi.digitech.file.processing.reader.text.service.TextPositionalReaderService;

import lombok.extern.slf4j.Slf4j;

@Service("textReaderService")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class TextReaderService extends AbstractReaderService<DataMap, MultipartFile> {
	
	private FileMetaData fileMetaData;

	private Map<String, Map<String, ColumnRelation>> sourceLineColumnRelationMap;
	
	@Autowired
	CustomReaderDynamicAutowireService customReaderDynamicAutowireService ;
	
	@Autowired(required = true)
	public TextReaderService(FileMetaData fileMetaData, List<ColumnRelation> columnRelations) {
		this.fileMetaData = fileMetaData;
		this.sourceLineColumnRelationMap = columnRelations.stream().collect(Collectors.groupingBy(
				ColumnRelation::getSourceKey, Collectors.toMap(ColumnRelation::getUuid, Function.identity())));
		
	}
	
	@Override
	public Map<String, List<DataMap>> read(MultipartFile data) throws ReaderException {
		
		var processing = fileMetaData.getProcessing();		
		
		try (var bufferReader = new BufferedReader(new InputStreamReader(data.getInputStream()));var bufferReaderCount = new BufferedReader(new InputStreamReader(data.getInputStream()))) {
			
			int lineCount = (int) bufferReaderCount.lines().count();
			fileMetaData.setTxtLinesCount(lineCount);
			
			var reader = new LineNumberReader(bufferReader);
			
			switch (ReadType.valueOf(processing.toUpperCase())) {
				case HEADER:
					return customReaderDynamicAutowireService.readTXTData(TextHeaderReaderService.class, reader, fileMetaData, sourceLineColumnRelationMap, requestContext);
				case POSITION:
					return customReaderDynamicAutowireService.readTXTData(TextPositionalReaderService.class, reader, fileMetaData, sourceLineColumnRelationMap, requestContext);
				case DELIMETER:
					return customReaderDynamicAutowireService.readTXTData(TextDelimetedReaderService.class, reader, fileMetaData, sourceLineColumnRelationMap, requestContext);
				default:
					return null;
			}
		} catch (IOException e) {
			log.error("Failed to read file: {}", data.getOriginalFilename(), e);
			throw new ReaderException("Error reading file: " + e.getMessage(), e, requestContext);
		}

	

	}

}