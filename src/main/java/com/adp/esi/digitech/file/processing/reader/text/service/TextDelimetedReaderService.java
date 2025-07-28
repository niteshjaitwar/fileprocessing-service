package com.adp.esi.digitech.file.processing.reader.text.service;

import java.io.IOException;
import java.io.LineNumberReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.file.processing.ds.config.model.FileMetaData;
import com.adp.esi.digitech.file.processing.ds.model.ColumnRelation;
import com.adp.esi.digitech.file.processing.exception.ReaderException;
import com.adp.esi.digitech.file.processing.model.DataMap;
import com.adp.esi.digitech.file.processing.util.ValidationUtil;

import lombok.extern.slf4j.Slf4j;


@Service("textDelimetedReaderService")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class TextDelimetedReaderService extends AbstractTextReaderService<DataMap, LineNumberReader> {
	
	@Autowired
	private TextDelimetedExtractService extractor;
	
	@Autowired(required = true)
	public TextDelimetedReaderService(FileMetaData fileMetaData, Map<String, Map<String, ColumnRelation>> sourceLineColumnRelationMap) {
		this.fileMetaData = fileMetaData;
		this.sourceLineColumnRelationMap = sourceLineColumnRelationMap;
		
	}

	@Override
	public Map<String, List<DataMap>> read(LineNumberReader reader) throws ReaderException {		
		
		try {
			extractor.extract(fileMetaData.getLines());
			
			var headerIndex = getLineCounter(fileMetaData.getStaticLines()) + getLineCounter(fileMetaData.getHeaders());
			var deleimeter = fileMetaData.getDelimeter();
			var sourceKey = fileMetaData.getSourceKey();
			var trailerCount = fileMetaData.getTrailer().length;
	
			Map<String, List<DataMap>> groupedDataMaps = new HashMap<>();
	
			String line;
			
	
			// No header skipping (headerIndex: -1)
			while ((line = reader.readLine()) != null) {
				var currentLineNumber = reader.getLineNumber();
				if (currentLineNumber <= headerIndex || !ValidationUtil.isHavingValue(line) || line.trim().isEmpty()) {
					log.debug("Skipping empty line at line number: {}", currentLineNumber);
					continue;
				}
				if(currentLineNumber > fileMetaData.getTxtLinesCount() - trailerCount)
					continue;
				
				var dataArray = line.split(deleimeter, -1);
	
				if (dataArray.length < 1) {
					log.debug("Invalid line at {}: {}", currentLineNumber, line);
					continue;
				}
	
				var currentLineMetadata = getLineMeatadata(dataArray, fileMetaData.getLines());
				var groupKey = getSourceKey(sourceKey, "{{", currentLineMetadata.getLineName(), "}}");
				var dataMap = lineReaderService.read(dataArray, currentLineMetadata, sourceLineColumnRelationMap.get(groupKey));
	
				groupedDataMaps.computeIfAbsent(groupKey, k -> new java.util.ArrayList<>()).add(dataMap);
			}
	
			return groupedDataMaps;
		} catch (IOException e) {
			log.error("Failed to read file: {}", "txt file", e);
			throw new ReaderException("Error reading file: " + e.getMessage(), e, requestContext);
		}

	}

}
