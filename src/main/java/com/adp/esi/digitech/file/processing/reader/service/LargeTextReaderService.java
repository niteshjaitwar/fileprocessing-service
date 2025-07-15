package com.adp.esi.digitech.file.processing.reader.service;

import java.io.IOException;
import java.io.LineNumberReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.file.processing.autowire.service.CustomReaderDynamicAutowireService;
import com.adp.esi.digitech.file.processing.ds.config.model.FileMetaData;
import com.adp.esi.digitech.file.processing.ds.model.ColumnRelation;
import com.adp.esi.digitech.file.processing.enums.ReadType;
import com.adp.esi.digitech.file.processing.exception.ReaderException;
import com.adp.esi.digitech.file.processing.model.ChunkDataMap;
import com.adp.esi.digitech.file.processing.reader.text.service.LargeTextDelimetedReaderService;
import com.adp.esi.digitech.file.processing.reader.text.service.LargeTextHeaderReaderService;
import com.adp.esi.digitech.file.processing.reader.text.service.LargeTextPositionalReaderService;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service("largeTextReaderService")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class LargeTextReaderService extends AbstractReaderService<ChunkDataMap, String> {
	
	private FileMetaData fileMetaData;

	private Map<String, Map<String, ColumnRelation>> sourceLineColumnRelationMap;	
	
	@Autowired
	CustomReaderDynamicAutowireService customReaderDynamicAutowireService ;
	
	@Autowired(required = true)
	public LargeTextReaderService(FileMetaData fileMetaData, List<ColumnRelation> columnRelations) {
		this.fileMetaData = fileMetaData;
		this.sourceLineColumnRelationMap = columnRelations.stream().collect(Collectors.groupingBy(
				ColumnRelation::getSourceKey, Collectors.toMap(ColumnRelation::getUuid, Function.identity())));
	}
	
	
	@Override
	public Map<String, List<ChunkDataMap>> read(String filePath) throws ReaderException {
		var processing = fileMetaData.getProcessing();	
		try(var bufferReader = Files.newBufferedReader(Paths.get(filePath), StandardCharsets.ISO_8859_1);
			var reader = new LineNumberReader(bufferReader)) {
			int linesCount = (int)Files.lines(Paths.get(filePath)).count();
			fileMetaData.setTxtLinesCount(linesCount);
			switch (ReadType.valueOf(processing.toUpperCase())) {
				case HEADER:
					return customReaderDynamicAutowireService.readLargeTXTData(LargeTextHeaderReaderService.class, reader, fileMetaData, sourceLineColumnRelationMap, requestContext);
				case POSITION:
					return customReaderDynamicAutowireService.readLargeTXTData(LargeTextPositionalReaderService.class, reader, fileMetaData, sourceLineColumnRelationMap, requestContext);
				case DELIMETER:
					return customReaderDynamicAutowireService.readLargeTXTData(LargeTextDelimetedReaderService.class, reader, fileMetaData, sourceLineColumnRelationMap, requestContext);
				default:
					return null;
			}
		} catch (IOException e) {
			log.error("Failed to read file: {}", "txt file", e);
       	 	throw new ReaderException(e.getMessage(), e, requestContext);
        }
	}
	
	
	
}