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
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

/**
 * Service for reading large XML files by batching, writing chunks to disk, and
 * returning chunk metadata.
 * 
 * Uses templates with complex structure including id, attrs arrays, child
 * arrays, and UUID placeholders from ColumnRelation objects stored in database.
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

		// Validate inputs
		if (filePath == null || filePath.trim().isEmpty()) {
			log.error("LargeXMLReaderService -> read() Received empty or null filePath, uniqueId = {}, sourceKey = {}",
					requestContext.getUniqueId(), fileMetaData.getSourceKey());
			var readerException = new ReaderException("File path cannot be null or empty");
			readerException.setRequestContext(requestContext);
			throw readerException;
		}
		
		if("PECI".equalsIgnoreCase(fileMetaData.getProcessingType()) && "Y".equalsIgnoreCase(fileMetaData.getFilterData())) {
			var payloadReductionService = payloadReductionServiceObjectProvider.getObject(objectMapper, requestContext);
			var outputPath = payloadReductionService.processXML(filePath, fileMetaData);
		}

		validateFileMetaData(fileMetaData);
		initializeTemplateMapping(fileMetaData);

		// Use batchSize from metadata
		int batchSize = fileMetaData.getBatchSize() > 0 ? fileMetaData.getBatchSize() : 1000;
		log.debug("LargeXMLReaderService -> read() Using batchSize: {} for sourceKey: {}", batchSize,
				fileMetaData.getSourceKey());

		Path path = Paths.get(filePath);
		if (!Files.exists(path)) {
			log.error("LargeXMLReaderService -> read() File does not exist at path = {}, uniqueId = {}, sourceKey = {}",
					filePath, requestContext.getUniqueId(), fileMetaData.getSourceKey());
			var readerException = new ReaderException("File does not exist at path: " + filePath);
			readerException.setRequestContext(requestContext);
			throw readerException;
		}

		Map<String, List<String>> metaData = new HashMap<>();
		int[] masterCount = { 0 };

		try (var reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
			@Cleanup
			XMLStreamReader xmlReader = createStreamReader(reader);

			// Create directory for chunks
			var dir = Paths
					.get(largeRequestFilePath + requestContext.getRequestUuid() + "/" + fileMetaData.getSourceKey());
			if (Files.notExists(dir)) {
				Files.createDirectories(dir);
				log.debug("LargeXMLReaderService -> read() Created directory for chunks at {}", dir);
			}

			List<DataMap> rows = new ArrayList<>();
			AtomicInteger fileCounter = new AtomicInteger();

			while (xmlReader.hasNext()) {
				int event = xmlReader.next();

				if (event == XMLStreamReader.START_ELEMENT && rootElementName.equals(xmlReader.getLocalName())) {

					// Initialize column map with all UUIDs set to null
					Map<UUID, String> columnMap = new HashMap<>();
					for (ColumnRelation relation : columnRelations) {
						columnMap.put(UUID.fromString(relation.getUuid()), null);
					}

					// Process root element attributes first
					String elementName = xmlReader.getLocalName();
					for (int i = 0; i < xmlReader.getAttributeCount(); i++) {
						String attrName = xmlReader.getAttributeLocalName(i);
						String attrValue = xmlReader.getAttributeValue(i);
						String attrPath = elementName + "@" + attrName;
						String uuid = templateUuidMap.get(attrPath);
						if (uuid != null) {
							columnMap.put(UUID.fromString(uuid), attrValue);
							log.debug("LargeXMLReaderService -> read() Mapped root attribute {} = {} to UUID {}",
									attrPath, attrValue, uuid);
						}
					}

					// Extract data from the entire XML element tree using sophisticated template
					// mapping
					Map<UUID, String> extractedData = extractDataFromXML(xmlReader, elementName);

					// Merge extracted data into column map
					extractedData.forEach((key, value) -> {
						if (value != null && !value.trim().isEmpty()) {
							columnMap.put(key, value);
						}
					});

					DataMap dataRow = new DataMap(columnMap);
					rows.add(dataRow);

					int mappedCount = (int) columnMap.entrySet().stream().mapToLong(e -> e.getValue() != null ? 1 : 0)
							.sum();
					log.debug(
							"LargeXMLReaderService -> read() Processed DataMap for {}: {} mapped values out of {} total columns",
							rootElementName, mappedCount, columnMap.size());

					if (rows.size() >= batchSize) {
						masterCount[0] += rows.size();

						var batchData = new ArrayList<>(rows);
						var fileName = fileMetaData.getSourceKey() + "_" + fileCounter.incrementAndGet();

						// Group by identifier if available
						Map<String, List<DataMap>> groupRows;
						List<String> keys;

						if (fileMetaData.getGroupIdentifier() != null
								&& !fileMetaData.getGroupIdentifier().trim().isEmpty()) {
							groupRows = batchData.parallelStream().collect(Collectors.groupingBy(row -> {
								String groupValue = row.getColumns()
										.get(UUID.fromString(fileMetaData.getGroupIdentifier()));
								return groupValue != null ? groupValue : "default";
							}));
							keys = new ArrayList<>(groupRows.keySet());
						} else {
							// No grouping, use single group
							groupRows = Map.of(fileName, batchData);
							keys = List.of(fileName);
						}

						// Async write
						CompletableFuture.runAsync(() -> {
							write(dir, fileName, groupRows);
							log.debug("LargeXMLReaderService -> read() Wrote batch {} with {} records", fileName,
									batchData.size());
						}, asyncExecutor);

						metaData.put(fileName, keys);
						rows.clear();
					}
				}
			}

			// Handle remaining rows
			if (!rows.isEmpty()) {
				masterCount[0] += rows.size();
				var fileName = fileMetaData.getSourceKey() + "_" + fileCounter.incrementAndGet();
				var batchData = new ArrayList<>(rows);

				Map<String, List<DataMap>> groupRows;
				List<String> keys;

				if (fileMetaData.getGroupIdentifier() != null && !fileMetaData.getGroupIdentifier().trim().isEmpty()) {
					groupRows = batchData.parallelStream().collect(Collectors.groupingBy(row -> {
						String groupValue = row.getColumns().get(UUID.fromString(fileMetaData.getGroupIdentifier()));
						return groupValue != null ? groupValue : "default";
					}));
					keys = new ArrayList<>(groupRows.keySet());
				} else {
					groupRows = Map.of(fileName, batchData);
					keys = List.of(fileName);
				}

				write(dir, fileName, groupRows);
				log.debug("LargeXMLReaderService -> read() Wrote final batch {} with {} records", fileName,
						batchData.size());
				metaData.put(fileName, keys);
				rows.clear();
			}

			// Write metadata
			CompletableFuture.runAsync(() -> {
				write(dir, "meta", metaData);
				log.debug("LargeXMLReaderService -> read() Wrote metadata file for sourceKey = {}",
						fileMetaData.getSourceKey());
			}, asyncExecutor);

			// Create source data map
			Map<String, List<String>> sourceDataMap = new HashMap<>();
			metaData.forEach((key, value) -> value
					.forEach(item -> sourceDataMap.computeIfAbsent(item, k -> new ArrayList<>()).add(key)));

			var chunks = sourceDataMap.entrySet().stream()
					.map(entry -> new ChunkDataMap(entry.getKey(), entry.getValue())).collect(Collectors.toList());

			var data = new HashMap<String, List<ChunkDataMap>>();
			data.put(fileMetaData.getSourceKey(), chunks);

			log.info(
					"LargeXMLReaderService -> read() Completed processing xml, uniqueId = {}, sourceKey = {}, Total Records = {}",
					requestContext.getUniqueId(), fileMetaData.getSourceKey(), masterCount[0]);
			return data;

		} catch (IOException | XMLStreamException e) {
			log.error(
					"LargeXMLReaderService -> read() Failed to process xml, uniqueId = {}, sourceKey = {}, message = {}",
					requestContext.getUniqueId(), fileMetaData.getSourceKey(), e.getMessage());
			var readerException = new ReaderException("XML Parsing failed, reason = " + e.getMessage(), e);
			readerException.setRequestContext(requestContext);
			throw readerException;
		}
	}
}
