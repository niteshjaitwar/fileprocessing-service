package com.adp.esi.digitech.file.processing.reader.text.service;

import java.io.IOException;
import java.io.LineNumberReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

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


@Service("textHeaderReaderService")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class TextHeaderReaderService extends AbstractTextReaderService<DataMap, LineNumberReader> {
	
	@Autowired
	private TextHeaderExtractService extractor;
	
	@Autowired(required = true)
	public TextHeaderReaderService(FileMetaData fileMetaData, Map<String, Map<String, ColumnRelation>> sourceLineColumnRelationMap) {
		this.fileMetaData = fileMetaData;
		this.sourceLineColumnRelationMap = sourceLineColumnRelationMap;
		
	}
	
	@Override
	public Map<String, List<DataMap>> read(LineNumberReader reader) throws ReaderException {
		
		try {		
			var staticLineIndex = getLineCounter(fileMetaData.getStaticLines());		
			var headerIndex = staticLineIndex + getLineCounter(fileMetaData.getLines());
			
			Map<String, List<DataMap>> groupedDataMaps = new HashMap<>();
			
			var deleimeter = fileMetaData.getDelimeter();
			var sourceKey = fileMetaData.getSourceKey();
			var trailerCount = fileMetaData.getTrailer().length;
			
					
			String line;
			
			var currentHeaderIndex = 0;
			while ((line = reader.readLine()) != null) {
				var currentLineNumber = reader.getLineNumber();
				if (currentLineNumber <= staticLineIndex || !ValidationUtil.isHavingValue(line) || line.trim().isEmpty()) {
					log.debug("Skipping empty line at line number: {}", currentLineNumber);
					continue;
				}
				
				if(currentLineNumber > fileMetaData.getTxtLinesCount() - trailerCount)
					continue;
				
				var dataArray = line.split(deleimeter, -1);
				
				if (currentLineNumber <= headerIndex) {	
					var currentLineMeatadata = fileMetaData.getLines()[currentHeaderIndex];
					var currentLinSourceKey = getSourceKey(fileMetaData.getSourceKey(), "{{", currentLineMeatadata.getLineName(), "}}");		
					var namedColumnRealtionsMap = sourceLineColumnRelationMap.get(currentLinSourceKey)
													.values()
													.stream()
													.collect(Collectors.toMap(cr -> cr.getColumnName(), Function.identity()));
					extractor.extract(currentLineMeatadata, dataArray, namedColumnRealtionsMap);
					currentHeaderIndex++;
					continue;
				}
				
				var currentLineMetadata = getLineMeatadata(dataArray, fileMetaData.getLines());
				var groupKey = getSourceKey(sourceKey, "{{", currentLineMetadata.getLineName(), "}}");
				var dataMap = lineReaderService.read(dataArray, currentLineMetadata.getColumnIdentifierPositions(), sourceLineColumnRelationMap.get(groupKey));
	
				groupedDataMaps.computeIfAbsent(groupKey, k -> new java.util.ArrayList<>()).add(dataMap);
				
			}
			
			return groupedDataMaps;
		} catch (IOException e) {
				log.error("Failed to read file: {}", "txt file", e);
				throw new ReaderException("Error reading file: " + e.getMessage(), e);
		}
	}

}
