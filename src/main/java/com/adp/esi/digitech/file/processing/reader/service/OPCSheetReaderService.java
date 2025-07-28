package com.adp.esi.digitech.file.processing.reader.service;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.util.XMLHelper;
import org.apache.poi.xssf.model.SharedStrings;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.adp.esi.digitech.file.processing.ds.config.model.FileMetaData;
import com.adp.esi.digitech.file.processing.ds.model.ColumnRelation;
import com.adp.esi.digitech.file.processing.exception.MetadataValidationException;
import com.adp.esi.digitech.file.processing.exception.ReaderException;
import com.adp.esi.digitech.file.processing.file.service.IFileService;
import com.adp.esi.digitech.file.processing.model.ChunkDataMap;
import com.adp.esi.digitech.file.processing.model.DataMap;
import com.adp.esi.digitech.file.processing.model.RequestContext;

import io.micrometer.common.util.StringUtils;
import lombok.extern.slf4j.Slf4j;

@Service("opcSheetReaderService")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class OPCSheetReaderService extends AbstractReaderService<ChunkDataMap, InputStream> {

	@Autowired
	ObjectProvider<IFileService> fileService;	

	private List<ColumnRelation> columnRelations;
	private FileMetaData fileMetaData;
	private SharedStrings sst;

	@Autowired(required = true)
	public OPCSheetReaderService(FileMetaData fileMetaData, List<ColumnRelation> columnRelations, SharedStrings sst) {
		this.fileMetaData = fileMetaData;
		this.columnRelations = columnRelations;
		this.sst = sst;
	}

	private SheetHandler getHandler(InputStream sheet, Path dir)
			throws SAXException, ParserConfigurationException, IOException {
		var parser = XMLHelper.newXMLReader();
		Function<ColumnRelation, String> keyFunc = item -> "position".equalsIgnoreCase(fileMetaData.getProcessing()) ? String.valueOf(item.getPosition()) : item.getColumnName(); 
		var columnRelationsMap = columnRelations.parallelStream()
				.collect(Collectors.toMap(keyFunc, Function.identity()));
		parser.setContentHandler(new SheetHandler(fileMetaData, columnRelationsMap, sst, requestContext, dir, asyncExecutor));
		var source = new InputSource(sheet);
		parser.parse(source);
		var sheetHandler = (SheetHandler) parser.getContentHandler();
		return sheetHandler;
	}

	@Override
	public Map<String, List<ChunkDataMap>> read(InputStream sheet) throws ReaderException {
		log.info("OPCSheetReaderService -> read() Started processing sheet, uniqueId = {}, sourceKey = {}",
				requestContext.getUniqueId(), fileMetaData.getSourceKey());
		try {
			var dir = Paths
					.get(largeRequestFilePath + requestContext.getRequestUuid() + "/" + fileMetaData.getSourceKey());

			if (Files.notExists(dir)) {
				Files.createDirectories(dir);
			}
			var sheetHandler = getHandler(sheet, dir);
			var sourceDataMap = sheetHandler.getSourceDataMap();

			log.info("OPCSheetReaderService -> read() Completed processing sheet, uniqueId = {}, sourceKey = {}",
					requestContext.getUniqueId(), fileMetaData.getSourceKey());
			return sourceDataMap;
		} catch (MetadataValidationException e) {
			log.error("OPCSheetReaderService -> read() Failed to processing sheet, uniqueId = {}, sourceKey = {}, message = {}",
					requestContext.getUniqueId(), fileMetaData.getSourceKey(), e.getMessage());
			throw e;
		} catch (Exception e) {
			log.error(
					"OPCSheetReaderService -> read() Failed to processing sheet, uniqueId = {}, sourceKey = {}, message = {}",
					requestContext.getUniqueId(), fileMetaData.getSourceKey(), e.getMessage());
			var readerException = new ReaderException("Sheet Parsing failed, reason = " + e.getMessage(), e.getCause());
			readerException.setRequestContext(requestContext);
			throw readerException;
		}
	}

	private class SheetHandler extends DefaultHandler {
		private final SharedStrings sst;
		private String lastContents;
		private boolean nextIsString;
		private boolean nextInlineString;
		private Map<String, String> headerMap = new HashMap<String, String>();
		private String headerKey;
		private Pattern pattern = Pattern.compile("^[A-Z]+");
		private int rowIndex;
		private int colSize;
		private boolean isHeaderIndex = false;
		private boolean isProcessRow = true;
		private Map<String, ColumnRelation> columnRelations;
		private Map<String, SimpleDateFormat> dateFormats;
		private FileMetaData fileMetaData;
		private RequestContext requestContext;
		private Executor asyncExecutor;
		
		
		
		private DataMap dataMap;
		private ColumnRelation tempColRel;
		private Map<UUID, String> columns;
		private DataMap clonedDataMap;
		List<DataMap> rows;
		int[] masterCount = { 0 };
		private Path dir;
		Map<String, List<String>> metaData = new HashMap<>();
		Map<String, List<ChunkDataMap>> sourceDataMap = new HashMap<>();
		AtomicInteger fileCounter = new AtomicInteger();

		public SheetHandler(FileMetaData fileMetaData, Map<String, ColumnRelation> columnRelations, SharedStrings sst,
				RequestContext requestContext, Path dir, Executor asyncExecutor) {		
			this.asyncExecutor = asyncExecutor;
			this.fileMetaData = fileMetaData;
			this.columnRelations = columnRelations;
			this.sst = sst;
			this.requestContext = requestContext;
			this.rows = new ArrayList<DataMap>();
			this.dir = dir;
		}
		
		public Map<String, List<ChunkDataMap>> getSourceDataMap() throws IOException {
			log.info("OPCSheetReaderService -> getSourceDataMap() Completed processing csv, uniqueId = {}, sourceKey = {}, Total Records = {}",
					requestContext.getUniqueId(), fileMetaData.getSourceKey(), masterCount[0]);
			return sourceDataMap;
		}

		Function<String, String> getKeyFunction = cellReference -> {
			var matcher = pattern.matcher(cellReference);
			return matcher.find() ? matcher.group() : "";
		};

		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes)
				throws SAXException {
			
			if (qName.equals("sheetData")) {
				dateFormats = getDateFormats(columnRelations.values().stream());
				if (POSITION.equalsIgnoreCase(fileMetaData.getProcessing())) {
					var dbHeaders = columnRelations.values().stream().map(item -> item.getPosition()).collect(Collectors.toList());
					columns = new HashMap<UUID, String>();
					dbHeaders.parallelStream().forEach(position -> columns.put(UUID.fromString(columnRelations.get(String.valueOf(position)).getUuid()), null));
					dataMap = new DataMap(columns);
					isHeaderIndex = true;
				}
			}
			
			if (qName.equals("row")) {
				rowIndex = Integer.valueOf(attributes.getValue("r"));
				isProcessRow = fileMetaData.getHeaderIndex() < rowIndex;
				if(!isProcessRow)
					return;
					
				if (!POSITION.equalsIgnoreCase(fileMetaData.getProcessing()) && fileMetaData.getHeaderIndex() + 1 == rowIndex)
					isHeaderIndex = true;
				else
					clonedDataMap = SerializationUtils.clone(dataMap);
			}

			if(!isProcessRow) 
				return;
			if (qName.equals("c")) {
				
				Function<String,ColumnRelation> columnRelationFun = columnName -> {
					return columnRelations.get(columnName);
				};
				
				headerKey = getKeyFunction.apply(attributes.getValue("r"));
				String cellType = attributes.getValue("t");
				nextIsString = cellType != null && cellType.equals("s");
				nextInlineString = cellType != null && cellType.equals("inlineStr");
				
				if(POSITION.equalsIgnoreCase(fileMetaData.getProcessing())) {
					var colIndex = getColumnIndex(headerKey);
					if(isHeaderIndex)
						colSize = colIndex;
					var columnRelation = columnRelationFun.apply(String.valueOf(colIndex));
					tempColRel = columnRelation;
				} else {
					if (!isHeaderIndex) {
						var columnName = headerMap.get(headerKey);
						var columnRelation = columnRelationFun.apply(columnName);
						tempColRel = columnRelation;
					}
				}
			}
			lastContents = "";
		}

		@Override
		public void endElement(String uri, String localName, String qName) throws SAXException {
			if(!isProcessRow)
				return;
			if (nextIsString) {
				var idx = Integer.parseInt(lastContents);
				lastContents = sst.getItemAt(idx).toString();
				nextIsString = false;
			}
			if (nextInlineString) {
				if (qName.equals("t")) {
					trimLastContents(lastContents);
				}
			}
			if (qName.equals("row")) {
				List<String> dbHeaders = columnRelations.values().parallelStream().map(item -> item.getColumnName()).collect(Collectors.toList());
				if(!POSITION.equalsIgnoreCase(fileMetaData.getProcessing()) && isHeaderIndex) {
					var excelHeaders = new ArrayList<String>(headerMap.values());
					OPCSheetReaderService.this.validate(fileMetaData.getSourceKey(), dbHeaders, excelHeaders);
					columns = new HashMap<UUID, String>();
					dbHeaders.parallelStream().forEach(column -> columns.put(UUID.fromString(columnRelations.get(column).getUuid()), null));
					dataMap = new DataMap(columns);
				}else if(POSITION.equalsIgnoreCase(fileMetaData.getProcessing()) && isHeaderIndex) {
					OPCSheetReaderService.this.validate(fileMetaData.getSourceKey(), dbHeaders.size(), colSize);
					isHeaderIndex = false;
				}
				
				if (!isHeaderIndex) {
					rows.add(clonedDataMap);
				} else {
					isHeaderIndex = false;
				}

				if (rows.size() >= fileMetaData.getBatchSize())
					prepareMetaData(false);

			} else if (qName.equals("v")) {
				trimLastContents(lastContents);
			} 
		}
		
		private void trimLastContents(String lastContents) {
			var finalContents = !StringUtils.isBlank(lastContents) ? lastContents.lines().collect(Collectors.joining(" ")).strip() : "";
			if (HEADER.equalsIgnoreCase(fileMetaData.getProcessing()) && isHeaderIndex) {
				headerMap.put(headerKey, finalContents);
			}
			else if(Objects.nonNull(tempColRel)) {
				if("DATE".equalsIgnoreCase(tempColRel.getDataType()) && StringUtils.isNotEmpty(finalContents) && NumberUtils.isParsable(lastContents)) {
					var sdf = dateFormats.get(tempColRel.getFormat());
					finalContents = sdf.format(DateUtil.getJavaDate(Double.valueOf(finalContents)));					
				}
				clonedDataMap.getColumns().put(UUID.fromString(tempColRel.getUuid()), finalContents);
			}
		}

		@Override
		public void characters(char ch[], int start, int length) throws SAXException {
			lastContents += new String(ch, start, length);
		}

		@Override
		public void endDocument() throws SAXException {
			if (!rows.isEmpty())
				prepareMetaData(false);

			CompletableFuture.runAsync(() -> {
				write(dir, "meta", metaData);
			}, asyncExecutor);
			Map<String, List<String>> temp = new HashMap<>();
			metaData.forEach((key, value) -> value
					.forEach(item -> temp.computeIfAbsent(item, k -> new ArrayList<>()).add(key)));
			
			var chunks= temp.entrySet().stream().map(entry -> new ChunkDataMap(entry.getKey(), entry.getValue())).collect(Collectors.toList());
			
			
			sourceDataMap.put(fileMetaData.getSourceKey(), chunks);
		}

		private void prepareMetaData(boolean async) {
			masterCount[0] += rows.size();
			var fileName = fileMetaData.getSourceKey() + "_" + fileCounter.incrementAndGet();
			var batchData = new ArrayList<>(rows);
			var groupRows = batchData.parallelStream().collect(Collectors
					.groupingBy(row -> row.getColumns().get(UUID.fromString(fileMetaData.getGroupIdentifier()))));
			var keys = groupRows.keySet().stream().collect(Collectors.toList());
			if(async) {
				CompletableFuture.runAsync(() -> {
					write(dir, fileName, groupRows);
				}, asyncExecutor);
			} else {
				write(dir, fileName, groupRows);
			}
			metaData.put(fileName, keys);
			rows.clear();
		}
		
		private static int getColumnIndex(String column) {
			int result = 0;
			for(char c : column.toCharArray()) {
				result = result * 26 + (c - 'A' +1);
			}
			return result;
			
		}
	}

}
