package com.adp.esi.digitech.file.processing.reader.service;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.file.processing.ds.config.model.FileMetaData;
import com.adp.esi.digitech.file.processing.ds.model.ColumnRelation;
import com.adp.esi.digitech.file.processing.exception.MetadataValidationException;
import com.adp.esi.digitech.file.processing.exception.ReaderException;
import com.adp.esi.digitech.file.processing.model.DataMap;

import lombok.extern.slf4j.Slf4j;



@Service("sheetReaderService")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class SheetReaderService extends AbstractReaderService<DataMap,XSSFSheet> {
	
	@Autowired
	DataFormatter dataFormatter;
	
	private FileMetaData fileMetaData;
	private List<ColumnRelation> columnRelations;	
	private Map<String, SimpleDateFormat> dateFormats;
	
	@Autowired(required = true)
	public SheetReaderService(FileMetaData fileMetaData, List<ColumnRelation> columnRelations) {
		this.fileMetaData = fileMetaData;	
		this.columnRelations = columnRelations;			
	}
	
	
	/*
	 * Points to consider 
	 * 1.Column Object Require additional fields like private String name, String uuid(uuid from ColumnRelation), String sourceKey(fileName$$sheetName$$)
	 */

	@Override
	public  Map<String, List<DataMap>> read(XSSFSheet sheet) throws ReaderException {
		
		try {
			log.info("SheetReaderService -> read() Started processing sheet, uniqueId = {}, sourceKey = {}", requestContext.getUniqueId(), fileMetaData.getSourceKey());
			List<DataMap> rows = new ArrayList<>();
			
			var headerIndex = fileMetaData.getHeaderIndex();	
			Map<String, Integer> headersCellMap = new LinkedHashMap<>();
			
			if(HEADER.equalsIgnoreCase(fileMetaData.getProcessing())) {
				var headersRow = sheet.getRow(headerIndex); 
				var headersCellIterator = headersRow.cellIterator();			
				
				while (headersCellIterator.hasNext()) {
					var cell = headersCellIterator.next();
					headersCellMap.put(dataFormatter.formatCellValue(cell), cell.getColumnIndex());			
				}
				validate(fileMetaData, columnRelations, new ArrayList<>(headersCellMap.keySet()));
			} else {
				var headersRow = sheet.getRow(0);
				validate(fileMetaData, columnRelations, headersRow);
			}
			dateFormats = getDateFormats(columnRelations.stream());
			
			var rowIterator = sheet.iterator();			
			while (rowIterator.hasNext()) {
				var row = rowIterator.next();
				if(HEADER.equalsIgnoreCase(fileMetaData.getProcessing()) && row.getRowNum() <= headerIndex)
					continue;
				
				Predicate<? super ColumnRelation> predicate = columnRelation -> POSITION.equalsIgnoreCase(fileMetaData.getProcessing()) ? Boolean.TRUE : headersCellMap.containsKey(columnRelation.getColumnName());
				
				var columns = columnRelations.parallelStream().filter(predicate)
												.collect(HashMap<UUID,String>::new, 
														(map,columnRelation) -> map.put(UUID.fromString(columnRelation.getUuid()), getValue(row, headersCellMap,columnRelation)), 
														HashMap<UUID,String>::putAll);
																								
				var dataRow = new DataMap(columns);
				rows.add(dataRow);
			}	
			var data = new HashMap<String, List<DataMap>>();
			
			data.put(fileMetaData.getSourceKey(),rows);
			log.info("SheetReaderService -> read() Completed processing sheet, uniqueId = {}, sourceKey = {}", requestContext.getUniqueId(), fileMetaData.getSourceKey());
			return  data;
		} catch (MetadataValidationException e) {
			log.error("SheetReaderService -> read() Failed to processing sheet, uniqueId = {}, sourceKey = {}, message = {}", requestContext.getUniqueId(), fileMetaData.getSourceKey(), e.getMessage());
			throw e;	
		} catch(Exception e) {
			log.error("SheetReaderService -> read() Failed to processing sheet, uniqueId = {}, sourceKey = {}, message = {}", requestContext.getUniqueId(), fileMetaData.getSourceKey(), e.getMessage());
			var readerException = new ReaderException("Sheet Parsing failed, reason = " + e.getMessage(), e.getCause());
			readerException.setRequestContext(requestContext);
			throw readerException;
		}
	}
	
	public  String getValue(Row row, Map<String, Integer> headersCellMap, ColumnRelation columnRelation) {
		
		if(POSITION.equalsIgnoreCase(fileMetaData.getProcessing()))
			return getCellValue(row.getCell(columnRelation.getPosition().intValue()-1),columnRelation);
		
		return getCellValue(row.getCell(headersCellMap.get(columnRelation.getColumnName())),columnRelation);
	}


	private boolean isCellEmpty(Cell cell) {
		if(cell == null || cell.getCellType() == CellType.BLANK)
			return true;
		
		if(cell.getCellType() == CellType.STRING && cell.getStringCellValue().trim().isEmpty())
			return true;
		
		return false;
	}
	
	private String getCellValue(Cell cell, ColumnRelation columnRelation) {
		if(!isCellEmpty(cell)) {
			if(cell.getCellType() == CellType.FORMULA) {			
				switch (cell.getCachedFormulaResultType()) {
					case BLANK:
						return null;
					case BOOLEAN:
						return String.valueOf(cell.getBooleanCellValue());
					case NUMERIC:
						if(DateUtil.isCellDateFormatted(cell)) {						
							return String.valueOf(cell.getDateCellValue());
						}
						return  String.valueOf(cell.getNumericCellValue());
					case STRING:
						return cell.getRichStringCellValue().getString();
					default:
						break;
				}
			} else if(cell.getCellType() == CellType.NUMERIC) {
				if(DateUtil.isCellDateFormatted(cell)) {
					if("DATE".equalsIgnoreCase(columnRelation.getDataType())) {
						var sdf = dateFormats.get(columnRelation.getFormat());
						return sdf.format(cell.getDateCellValue());
					}
					return String.valueOf(cell.getDateCellValue());
				}
				return  dataFormatter.formatCellValue(cell);
			} else {
				
				return dataFormatter.formatCellValue(cell);
			}
		}
		return null;
	}
	
	public void validate(FileMetaData fileMetaData, List<ColumnRelation> columnRelations, Row row) throws MetadataValidationException {		
		var rowIndexSize = row.getLastCellNum();
		this.validate(fileMetaData.getSourceKey(), columnRelations.size(), rowIndexSize);
	}
	
	public void validate(FileMetaData fileMetaData, List<ColumnRelation> columnRelations, List<String> reqHeaders) throws MetadataValidationException {
		var dbHeaders = columnRelations.stream().map(ColumnRelation::getColumnName).toList();
		this.validate(fileMetaData.getSourceKey(), dbHeaders, reqHeaders);
	}


}
