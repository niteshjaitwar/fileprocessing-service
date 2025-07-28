package com.adp.esi.digitech.file.processing.reader.xml.service;

import java.io.IOException;
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

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

/**
 * Service for reading small XML files in memory and converting them to DataMap
 * objects.
 * 
 * Uses templates with complex structure including id, attrs arrays, child
 * arrays, and UUID placeholders in {{uuid}} format from ColumnRelation objects.
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

		// Validate inputs
		if (file == null || file.isEmpty()) {
			log.error("XMLReaderService -> read() Received empty or null file, uniqueId = {}, sourceKey = {}",
					requestContext.getUniqueId(), fileMetaData.getSourceKey());
			var readerException = new ReaderException("XML file cannot be null or empty");
			readerException.setRequestContext(requestContext);
			throw readerException;
		}
		
		

		validateFileMetaData(fileMetaData);
		initializeTemplateMapping(fileMetaData);

		try (var inputStream = file.getInputStream()) {
			if("PECI".equalsIgnoreCase(fileMetaData.getProcessingType()) && "Y".equalsIgnoreCase(fileMetaData.getFilterData())) {
				var payloadReductionService = payloadReductionServiceObjectProvider.getObject(objectMapper, requestContext);
				
				var processedStream = payloadReductionService.processXML(inputStream, fileMetaData);
			}
			
			
			@Cleanup
			XMLStreamReader xmlReader = createStreamReader(inputStream);
			List<DataMap> rows = new ArrayList<>();

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
							log.debug("XMLReaderService -> read() Mapped root attribute {} = {} to UUID {}", attrPath,
									attrValue, uuid);
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

					rows.add(new DataMap(columnMap));

					int mappedCount = (int) columnMap.entrySet().stream().mapToLong(e -> e.getValue() != null ? 1 : 0)
							.sum();
					log.debug(
							"XMLReaderService -> read() Processed DataMap for {}: {} mapped values out of {} total columns",
							rootElementName, mappedCount, columnMap.size());
				}
			}

			log.info(
					"XMLReaderService -> read() Completed processing xml, uniqueId = {}, sourceKey = {}, totalRecords = {}",
					requestContext.getUniqueId(), fileMetaData.getSourceKey(), rows.size());
			return Map.of(fileMetaData.getSourceKey(), rows);

		} catch (IOException | XMLStreamException e) {
			log.error("XMLReaderService -> read() Failed to process xml, uniqueId = {}, sourceKey = {}, message = {}",
					requestContext.getUniqueId(), fileMetaData.getSourceKey(), e.getMessage());
			var readerException = new ReaderException("XML Parsing failed, reason = " + e.getMessage(), e.getCause());
			readerException.setRequestContext(requestContext);
			throw readerException;
		}
	}
}
