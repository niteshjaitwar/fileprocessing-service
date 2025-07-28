package com.adp.esi.digitech.file.processing.reader.xml.service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.file.processing.ds.config.model.FileMetaData;
import com.adp.esi.digitech.file.processing.ds.model.ColumnRelation;
import com.adp.esi.digitech.file.processing.exception.ReaderException;
import com.adp.esi.digitech.file.processing.model.ChunkDataMap;
import com.adp.esi.digitech.file.processing.model.DataMap;
import com.adp.esi.digitech.file.processing.util.ValidationUtil;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

/**
 * Service for reading large XML files by batching, writing chunks to disk, and
 * returning chunk metadata grouped by sourceKey.
 * 
 * Enhanced to support sourceKey-based grouping, primary identifier handling,
 * and payload reduction for PECI processing.
 * 
 * @author rhidau
 */
@Service("largeXmlReaderService")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class LargeXMLReaderService extends AbstractXMLReaderService<ChunkDataMap, String> {

	@Autowired
	ObjectMapper objectMapper;

	@Autowired
	Executor asyncExecutor;

	private final List<ColumnRelation> columnRelations;
	private final FileMetaData fileMetaData;

	@Autowired(required = true)
	public LargeXMLReaderService(FileMetaData fileMetaData, List<ColumnRelation> columnRelations) {
		this.fileMetaData = fileMetaData;
		this.columnRelations = columnRelations;
	}

	@Override
	public Map<String, List<ChunkDataMap>> read(String filePath) throws ReaderException {
		log.info("LargeXMLReaderService -> read() Started processing xml, uniqueId = {}, sourceKey = {}",
				requestContext.getUniqueId(), fileMetaData.getSourceKey());

		if (filePath == null || filePath.trim().isEmpty()) {
			log.error("LargeXMLReaderService -> read() Received empty or null filePath, uniqueId = {}, sourceKey = {}",
					requestContext.getUniqueId(), fileMetaData.getSourceKey());
			var readerException = new ReaderException("File path cannot be null or empty");
			readerException.setRequestContext(requestContext);
			throw readerException;
		}

		String filePathToUse = filePath;

		if ("PECI".equalsIgnoreCase(fileMetaData.getProcessingType())
				&& "Y".equalsIgnoreCase(fileMetaData.getFilterData())) {

			var payloadReductionService = payloadReductionServiceObjectProvider.getObject(objectMapper, requestContext);
			Path processedPath = payloadReductionService.processXML(filePath, fileMetaData);
			filePathToUse = processedPath.toString();
			log.info("LargeXMLReaderService -> read() Applied payload reduction for PECI processing, new path: {}",
					filePathToUse);
		}

		validateFileMetaData(fileMetaData);
		initializeTemplateMapping(fileMetaData, columnRelations);

		int batchSize = fileMetaData.getBatchSize() > 0 ? fileMetaData.getBatchSize() : 1000;
		log.debug("LargeXMLReaderService -> read() Using batchSize: {} for sourceKey: {}", batchSize,
				fileMetaData.getSourceKey());

		Path path = Paths.get(filePathToUse);
		if (!Files.exists(path)) {
			log.error("LargeXMLReaderService -> read() File does not exist at path = {}, uniqueId = {}, sourceKey = {}",
					filePathToUse, requestContext.getUniqueId(), fileMetaData.getSourceKey());
			var readerException = new ReaderException("File does not exist at path: " + filePathToUse);
			readerException.setRequestContext(requestContext);
			throw readerException;
		}

		Map<String, Map<String, List<String>>> sourceKeyMetaData = new HashMap<>();
		Map<String, Integer> sourceKeyRecordCounts = new HashMap<>();

		for (String sourceKey : sourceKeyColumnRelationMap.keySet()) {
			sourceKeyMetaData.put(sourceKey, new HashMap<>());
			sourceKeyRecordCounts.put(sourceKey, 0);
		}

		try (var reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
			@Cleanup
			XMLStreamReader xmlReader = createStreamReader(reader);

			var dir = Paths
					.get(largeRequestFilePath + requestContext.getRequestUuid() + "/" + fileMetaData.getSourceKey());
			if (Files.notExists(dir)) {
				Files.createDirectories(dir);
				log.debug("LargeXMLReaderService -> read() Created directory for chunks at {}", dir);
			}

			Map<String, List<DataMap>> sourceKeyBatches = new HashMap<>();
			Map<String, AtomicInteger> sourceKeyFileCounters = new HashMap<>();

			for (String sourceKey : sourceKeyColumnRelationMap.keySet()) {
				sourceKeyBatches.put(sourceKey, new ArrayList<>());
				sourceKeyFileCounters.put(sourceKey, new AtomicInteger());
			}

			while (xmlReader.hasNext()) {
				int event = xmlReader.next();

				if (event == XMLStreamReader.START_ELEMENT && rootElementName.equals(xmlReader.getLocalName())) {

					Map<String, Map<UUID, String>> groupedData = extractDataFromXMLGrouped(xmlReader, rootElementName);

					for (Map.Entry<String, Map<UUID, String>> entry : groupedData.entrySet()) {
						String sourceKey = entry.getKey();
						Map<UUID, String> columnMap = entry.getValue();
						boolean hasData = columnMap.values().stream().anyMatch(ValidationUtil::isHavingValue);

						if (hasData) {
							DataMap dataRow = new DataMap(columnMap);
							sourceKeyBatches.get(sourceKey).add(dataRow);
							sourceKeyRecordCounts.put(sourceKey, sourceKeyRecordCounts.get(sourceKey) + 1);

							int mappedCount = (int) columnMap.entrySet().stream()
									.mapToLong(e -> ValidationUtil.isHavingValue(e.getValue()) ? 1 : 0).sum();
							log.debug(
									"LargeXMLReaderService -> read() Added DataMap to sourceKey {}: {} mapped values out of {} total columns",
									sourceKey, mappedCount, columnMap.size());

							List<DataMap> currentBatch = sourceKeyBatches.get(sourceKey);
							if (currentBatch.size() >= batchSize) {
								writeBatchForSourceKey(dir, sourceKey, currentBatch,
										sourceKeyFileCounters.get(sourceKey), sourceKeyMetaData.get(sourceKey),
										fileMetaData);
								currentBatch.clear();
							}
						}
					}
				}
			}

			for (String sourceKey : sourceKeyColumnRelationMap.keySet()) {
				List<DataMap> remainingBatch = sourceKeyBatches.get(sourceKey);
				if (!remainingBatch.isEmpty()) {
					writeBatchForSourceKey(dir, sourceKey, remainingBatch, sourceKeyFileCounters.get(sourceKey),
							sourceKeyMetaData.get(sourceKey), fileMetaData);
					remainingBatch.clear();
				}
			}

			for (String sourceKey : sourceKeyColumnRelationMap.keySet()) {
				Map<String, List<String>> metaData = sourceKeyMetaData.get(sourceKey);
				if (!metaData.isEmpty()) {
					CompletableFuture.runAsync(() -> {
						write(dir, sourceKey + "_meta", metaData);
						log.debug("LargeXMLReaderService -> read() Wrote metadata file for sourceKey = {}", sourceKey);
					}, asyncExecutor);
				}
			}

			Map<String, List<ChunkDataMap>> resultMap = new HashMap<>();

			for (String sourceKey : sourceKeyColumnRelationMap.keySet()) {
				Map<String, List<String>> metaData = sourceKeyMetaData.get(sourceKey);

				Map<String, List<String>> sourceDataMap = new HashMap<>();
				metaData.forEach((key, value) -> value
						.forEach(item -> sourceDataMap.computeIfAbsent(item, k -> new ArrayList<>()).add(key)));

				var chunks = sourceDataMap.entrySet().stream()
						.map(entry -> new ChunkDataMap(entry.getKey(), entry.getValue())).collect(Collectors.toList());

				resultMap.put(sourceKey, chunks);

				log.info("LargeXMLReaderService -> read() SourceKey {} has {} chunks with {} total records", sourceKey,
						chunks.size(), sourceKeyRecordCounts.get(sourceKey));
			}

			int totalRecords = sourceKeyRecordCounts.values().stream().mapToInt(Integer::intValue).sum();
			log.info(
					"LargeXMLReaderService -> read() Completed processing xml, uniqueId = {}, totalRecords = {}, sourceKeys = {}",
					requestContext.getUniqueId(), totalRecords, resultMap.keySet());

			return resultMap;

		} catch (IOException | XMLStreamException e) {
			log.error(
					"LargeXMLReaderService -> read() Failed to process xml, uniqueId = {}, sourceKey = {}, message = {}",
					requestContext.getUniqueId(), fileMetaData.getSourceKey(), e.getMessage());
			var readerException = new ReaderException("XML Parsing failed, reason = " + e.getMessage(), e);
			readerException.setRequestContext(requestContext);
			throw readerException;
		}
	}

	/**
	 * Writes a batch of data for a specific sourceKey to disk. Follows the same
	 * pattern as LargeCSVReaderService.
	 */
	private void writeBatchForSourceKey(Path dir, String sourceKey, List<DataMap> batchData, AtomicInteger fileCounter,
			Map<String, List<String>> metaData, FileMetaData fileMetaData) {

		var fileName = sourceKey + "_" + fileCounter.incrementAndGet();
		var batchDataCopy = new ArrayList<>(batchData);

		Map<String, List<DataMap>> groupRows;
		List<String> keys;

		if (ValidationUtil.isHavingValue(fileMetaData.getGroupIdentifier())) {
			groupRows = batchDataCopy.parallelStream().collect(Collectors.groupingBy(row -> {
				String groupValue = row.getColumns().get(UUID.fromString(fileMetaData.getGroupIdentifier()));
				return ValidationUtil.isHavingValue(groupValue) ? groupValue : "default";
			}));
			keys = new ArrayList<>(groupRows.keySet());
		} else {
			groupRows = Map.of(fileName, batchDataCopy);
			keys = List.of(fileName);
		}

		CompletableFuture.runAsync(() -> {
			write(dir, fileName, groupRows);
			log.debug(
					"LargeXMLReaderService -> writeBatchForSourceKey() Wrote batch {} for sourceKey {} with {} records",
					fileName, sourceKey, batchDataCopy.size());
		}, asyncExecutor);

		metaData.put(fileName, keys);
	}
}
