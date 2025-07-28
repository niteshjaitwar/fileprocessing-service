package com.adp.esi.digitech.file.processing.reader.text.service;

import java.io.IOException;
import java.io.LineNumberReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.file.processing.ds.config.model.FileMetaData;
import com.adp.esi.digitech.file.processing.ds.config.model.LineMetaData;
import com.adp.esi.digitech.file.processing.ds.model.ColumnRelation;
import com.adp.esi.digitech.file.processing.exception.ReaderException;
import com.adp.esi.digitech.file.processing.model.ChunkDataMap;
import com.adp.esi.digitech.file.processing.model.DataMap;
import com.adp.esi.digitech.file.processing.util.ValidationUtil;

import lombok.extern.slf4j.Slf4j;

@Service
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class LargeTextPositionalReaderService   extends AbstractTextReaderService<ChunkDataMap, LineNumberReader>{
	
	@Autowired
	private TextPositionalExtractService extractor;
	
	@Autowired(required = true)
	public LargeTextPositionalReaderService(FileMetaData fileMetaData, Map<String, Map<String, ColumnRelation>> sourceLineColumnRelationMap) {
		this.fileMetaData = fileMetaData;
		this.sourceLineColumnRelationMap = sourceLineColumnRelationMap;		
	}

	@Override
	public Map<String, List<ChunkDataMap>> read(LineNumberReader reader) throws ReaderException {
		try {
			extractor.extract(fileMetaData.getLines());		
			var headerIndex = getLineCounter(fileMetaData.getStaticLines()) + getLineCounter(fileMetaData.getHeaders());
			var sourceKey = fileMetaData.getSourceKey();
			var trailerCount = fileMetaData.getTrailer().length;
			
			var groupedDataMaps = new HashMap<String, List<DataMap>>();
			
			var lineInfo = getLineInfo(fileMetaData.getLines());
			
			@SuppressWarnings("unchecked")
			var groupedFileCounter = (Map<String, AtomicInteger>) lineInfo[0];
			
			@SuppressWarnings("unchecked")
			var groupedMetadata = (Map<String, Map<String, List<String>>>) lineInfo[1];
			
			@SuppressWarnings("unchecked")
			var lineMetadataMap = (Map<String, LineMetaData>) lineInfo[2];
			
			var dataCounter = 0;
			
			String line;		
			
			while ((line = reader.readLine()) != null) {
				var currentLineNumber = reader.getLineNumber();
				if (currentLineNumber <= headerIndex || !ValidationUtil.isHavingValue(line) || line.trim().isEmpty()) {
					log.debug("Skipping empty line at line number: {}", currentLineNumber);
					continue;
				}
				
				if(currentLineNumber > fileMetaData.getTxtLinesCount() - trailerCount)
					continue;
				
				var currentLineMetadata = getLineMeatadata(line, fileMetaData.getLines());
				var groupKey = getSourceKey(sourceKey, "{{", currentLineMetadata.getLineName(), "}}");
				var dataMap = lineReaderService.read(line, currentLineMetadata, sourceLineColumnRelationMap.get(groupKey));
	
				groupedDataMaps.computeIfAbsent(groupKey, k -> new java.util.ArrayList<>()).add(dataMap);
				dataCounter ++;
				
				if(dataCounter  >= fileMetaData.getBatchSize()) {
					write(groupedDataMaps, groupedFileCounter, groupedMetadata, lineMetadataMap, false);
					dataCounter = 0;
				}				
			}
			
			write(groupedDataMaps, groupedFileCounter, groupedMetadata, lineMetadataMap, true);
			
			writeMetadata(groupedMetadata);
			
			return getLineDataChunks(groupedMetadata);
			
		}  catch (IOException e) {
			log.error("Failed to read file: {}", "txt file", e);
			throw new ReaderException("Error reading file: " + e.getMessage(), e, requestContext);
		}
	}

	
}