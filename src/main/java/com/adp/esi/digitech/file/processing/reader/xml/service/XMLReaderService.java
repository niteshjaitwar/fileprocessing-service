package com.adp.esi.digitech.file.processing.reader.xml.service;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.adp.esi.digitech.file.processing.ds.config.model.FileMetaData;
import com.adp.esi.digitech.file.processing.ds.model.ColumnRelation;
import com.adp.esi.digitech.file.processing.exception.ReaderException;
import com.adp.esi.digitech.file.processing.model.DataMap;
import com.adp.esi.digitech.file.processing.util.ValidationUtil;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

/**
 * Service for reading small XML files in memory and converting them to DataMap
 * objects grouped by sourceKey.
 * 
 * Enhanced to support sourceKey-based grouping, primary identifier handling,
 * and payload reduction for PECI processing.
 *
 * @author rhidau
 */
@Service("xmlReaderService")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class XMLReaderService extends AbstractXMLReaderService<DataMap, MultipartFile> {

	private final List<ColumnRelation> columnRelations;
	private final FileMetaData fileMetaData;

	@Autowired(required = true)
	public XMLReaderService(FileMetaData fileMetaData, List<ColumnRelation> columnRelations) {
		this.fileMetaData = fileMetaData;
		this.columnRelations = columnRelations;
	}

	@Override
	public Map<String, List<DataMap>> read(MultipartFile file) throws ReaderException {
		log.info("XMLReaderService -> read() Started processing xml, uniqueId = {}, sourceKey = {}",
				requestContext.getUniqueId(), fileMetaData.getSourceKey());

		if (file == null || file.isEmpty()) {
			log.error("XMLReaderService -> read() Received empty or null file, uniqueId = {}, sourceKey = {}",
					requestContext.getUniqueId(), fileMetaData.getSourceKey());
			var readerException = new ReaderException("XML file cannot be null or empty");
			readerException.setRequestContext(requestContext);
			throw readerException;
		}

		validateFileMetaData(fileMetaData);
		initializeTemplateMapping(fileMetaData, columnRelations);

		InputStream inputStreamToUse = null;

		try {
			inputStreamToUse = file.getInputStream();

			if ("PECI".equalsIgnoreCase(fileMetaData.getProcessingType())
					&& "Y".equalsIgnoreCase(fileMetaData.getFilterData())) {

				var payloadReductionService = payloadReductionServiceObjectProvider.getObject(objectMapper,
						requestContext);
				inputStreamToUse = payloadReductionService.processXML(inputStreamToUse, fileMetaData);
				log.info("XMLReaderService -> read() Applied payload reduction for PECI processing");
			}

			@Cleanup
			XMLStreamReader xmlReader = createStreamReader(inputStreamToUse);

			Map<String, List<DataMap>> resultMap = new HashMap<>();

			for (String sourceKey : sourceKeyColumnRelationMap.keySet()) {
				resultMap.put(sourceKey, new ArrayList<>());
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
							DataMap dataMap = new DataMap(columnMap);
							resultMap.get(sourceKey).add(dataMap);

							int mappedCount = (int) columnMap.entrySet().stream()
									.mapToLong(e -> ValidationUtil.isHavingValue(e.getValue()) ? 1 : 0).sum();
							log.debug(
									"XMLReaderService -> read() Added DataMap to sourceKey {}: {} mapped values out of {} total columns",
									sourceKey, mappedCount, columnMap.size());
						}
					}
				}
			}

			int totalRecords = resultMap.values().stream().mapToInt(List::size).sum();
			log.info(
					"XMLReaderService -> read() Completed processing xml, uniqueId = {}, totalRecords = {}, sourceKeys = {}",
					requestContext.getUniqueId(), totalRecords, resultMap.keySet());

			for (Map.Entry<String, List<DataMap>> entry : resultMap.entrySet()) {
				log.info("XMLReaderService -> read() SourceKey {} has {} records", entry.getKey(),
						entry.getValue().size());
			}

			return resultMap;

		} catch (IOException | XMLStreamException e) {
			log.error("XMLReaderService -> read() Failed to process xml, uniqueId = {}, sourceKey = {}, message = {}",
					requestContext.getUniqueId(), fileMetaData.getSourceKey(), e.getMessage());
			var readerException = new ReaderException("XML Parsing failed, reason = " + e.getMessage(), e.getCause());
			readerException.setRequestContext(requestContext);
			throw readerException;
		} finally {
			if (inputStreamToUse != null) {
				try {
					InputStream originalStream = file.getInputStream();
					if (inputStreamToUse != originalStream) {
						inputStreamToUse.close();
					}
				} catch (IOException e) {
					log.warn("XMLReaderService -> read() Failed to close processed input stream: {}", e.getMessage());
				}
			}
		}
	}
}
