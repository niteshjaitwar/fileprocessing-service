package com.adp.esi.digitech.file.processing.reader.text.service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Autowired;

import com.adp.esi.digitech.file.processing.ds.config.model.FileMetaData;
import com.adp.esi.digitech.file.processing.ds.config.model.LineMetaData;
import com.adp.esi.digitech.file.processing.ds.model.ColumnRelation;
import com.adp.esi.digitech.file.processing.exception.ReaderException;
import com.adp.esi.digitech.file.processing.model.ChunkDataMap;
import com.adp.esi.digitech.file.processing.model.DataMap;
import com.adp.esi.digitech.file.processing.reader.service.AbstractReaderService;
import com.adp.esi.digitech.file.processing.util.ValidationUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractTextReaderService<T, V> extends AbstractReaderService<T, V> {

	protected LineReaderService lineReaderService;

	protected FileMetaData fileMetaData;

	protected Map<String, Map<String, ColumnRelation>> sourceLineColumnRelationMap;

	@Autowired
	protected void setLineReaderService(LineReaderService lineReaderService) {
		this.lineReaderService = lineReaderService;
	}

	protected int getLineCounter(Object[] lines) {
		return Objects.nonNull(lines) ? lines.length : 0;
	}

	protected String getSourceKey(String... items) {
		return Stream.of(items).collect(Collectors.joining(""));
	}

	protected LineMetaData getLineMeatadata(String[] data, LineMetaData[] lineMetaDataArray) {
		return Stream.of(lineMetaDataArray).filter(lineMetadata -> {
			var lineIdentifiers = lineMetadata.getLineIdentifiers();
			return lineIdentifiers.stream().allMatch(lineIdentifier -> {
				var column = lineIdentifier[0];
				var position = Integer.parseInt(lineIdentifier[1]);
				return column.equals(data[position]);
			});
		}).findFirst().orElseThrow(() -> {
			log.error("AbstractTextReaderService -> read() Failed to processing txt, uniqueId = {}, fileName = {}, message = {}",
					requestContext.getUniqueId(), fileMetaData.getSourceKey(), "No matched line metadata");
			var readerException = new ReaderException("Text file Parsing failed, reason = No matched line metadata");
			readerException.setRequestContext(requestContext);
			return readerException;

		});
	}

	protected LineMetaData getLineMeatadata(String line, LineMetaData[] lineMetaDataArray) {
		return Stream.of(lineMetaDataArray).filter(lineMetadata -> {
			var lineIdentifiers = lineMetadata.getLineIdentifiers();
			return lineIdentifiers.stream().allMatch(lineIdentifier -> {
				var column = lineIdentifier[0];
				var startPosition = Integer.parseInt(lineIdentifier[1]);
				var endPosition = Integer.parseInt(lineIdentifier[2]);
				var code = line.substring(startPosition, endPosition);
				return column.equals(code);
			});
		}).findFirst().orElseThrow(() -> {
			log.error("AbstractTextReaderService -> read() Failed to processing txt, uniqueId = {}, fileName = {}, message = {}",
					requestContext.getUniqueId(), fileMetaData.getSourceKey(), "No matched line metadata");
			var readerException = new ReaderException("Text file Parsing failed, reason = No matched line metadata");
			readerException.setRequestContext(requestContext);
			return readerException;

		});
	}
	
	protected Object[] getLineInfo(LineMetaData[] lineMetadataArray) {		
		var atomicIntegerMap = new HashMap<String, AtomicInteger>();
		var fileMetadataMap = new HashMap<String, Map<String, List<String>>>();
		var lineMetadataMap = new HashMap<String, LineMetaData>();
		for (int i = 0; i < lineMetadataArray.length; i++) {
			var lineMetadata = lineMetadataArray[i];
			var sourceKey = getSourceKey(fileMetaData.getSourceKey(), "{{", lineMetadata.getLineName(), "}}");
			atomicIntegerMap.put(sourceKey, new AtomicInteger());
			fileMetadataMap.put(sourceKey, new HashMap<String, List<String>>());
			lineMetadataMap.put(sourceKey, lineMetadata);
		}
		return new Object[] {atomicIntegerMap, fileMetadataMap, lineMetadataMap};		
	}
			
			
	protected Map<String, AtomicInteger> getFileCounter(LineMetaData[] lineMetaDataArray) {
		return Stream.of(lineMetaDataArray).collect(Collectors.toMap(
				lineMetaData -> getSourceKey(fileMetaData.getSourceKey(), "{{", lineMetaData.getLineName(), "}}"),
				lineMetaData -> new AtomicInteger()));
	}
	
	protected Map<String, Map<String, List<String>>> getFileMetadata(LineMetaData[] lineMetaDataArray) {
		return Stream.of(lineMetaDataArray).collect(Collectors.toMap(
				lineMetaData -> getSourceKey(fileMetaData.getSourceKey(), "{{", lineMetaData.getLineName(), "}}"),
				lineMetaData -> new HashMap<String, List<String>>()));
	}
	
	protected Map<String, LineMetaData> getLineMetadataMap(LineMetaData[] lineMetaDataArray) {
		return Stream.of(lineMetaDataArray).collect(Collectors.toMap(
				lineMetaData -> getSourceKey(fileMetaData.getSourceKey(), "{{", lineMetaData.getLineName(), "}}"),
				Function.identity()));
	}
	
	protected void write(Map<String, List<DataMap>> dataMap, Map<String, AtomicInteger> fileCounterMap, Map<String, Map<String, List<String>>> metadataMap, Map<String, LineMetaData> lineMetadataMap, boolean isWriteRemaining) throws IOException {
		for (Map.Entry<String, List<DataMap>> entry : dataMap.entrySet()) {
			String sourceKey = entry.getKey();
			List<DataMap> rows = entry.getValue();
			var fileCounter = fileCounterMap.get(sourceKey);
			var metaData = metadataMap.get(sourceKey);
		    var lineMetadata = lineMetadataMap.get(sourceKey); 
			var dir = Paths.get(largeRequestFilePath + requestContext.getRequestUuid() + "/" + sourceKey);		    	
		    if(Files.notExists(dir)) {
		    	Files.createDirectories(dir);
		    }
		    
		    if((Objects.nonNull(rows) && !rows.isEmpty()) && (rows.size() >= fileMetaData.getBatchSize() || isWriteRemaining)) {					
				
				var batchData = new ArrayList<>(rows);				
				var fileName = sourceKey+ "_" + fileCounter.incrementAndGet();
				String groupIdentifier = lineMetadata.getGroupIdentifier();
				if (ValidationUtil.isHavingValue(groupIdentifier) && groupIdentifier.startsWith("{{") && groupIdentifier.endsWith("}}"))
					groupIdentifier = groupIdentifier.substring(2, groupIdentifier.length() - 2);
				final var finalGroupIdentifier = groupIdentifier;
				var groupRows = batchData.parallelStream().collect(Collectors.groupingBy(row -> row.getColumns().get(UUID.fromString(finalGroupIdentifier))));
				var keys = groupRows.keySet().stream().collect(Collectors.toList());					
				
				CompletableFuture.runAsync(() -> {						
					write(dir, fileName, groupRows);
				}, asyncExecutor);
				
				metaData.put(fileName, keys);
				rows.clear();
			}		       
		}			
	}
	
	protected void writeMetadata(Map<String, Map<String, List<String>>> groupedMetadata) {
		for (Map.Entry<String, Map<String, List<String>>> entry : groupedMetadata.entrySet()) {
			var metaData = entry.getValue();
			var sourceKey = entry.getKey();
			var dir = Paths.get(largeRequestFilePath + requestContext.getRequestUuid() + "/" + sourceKey);
			CompletableFuture.runAsync(() -> {
				write(dir, "meta", metaData);
			}, asyncExecutor);
		}
	}
	
	protected Map<String, List<ChunkDataMap>> getLineDataChunks(Map<String, Map<String, List<String>>> groupedMetadata) {
		return groupedMetadata.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entryMetada -> {
			var metaData = entryMetada.getValue();
			
			Map<String, List<String>> sourceDataMap = new HashMap<>();
			metaData.forEach((key, value) -> value.forEach(item -> sourceDataMap.computeIfAbsent(item, k -> new ArrayList<>()).add(key)));
			
			var chunks= sourceDataMap.entrySet().stream().map(entry -> new ChunkDataMap(entry.getKey(), entry.getValue())).collect(Collectors.toList());
			
			return chunks;
		}));
	}
	
	
}
