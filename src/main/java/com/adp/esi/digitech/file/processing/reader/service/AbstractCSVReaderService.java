package com.adp.esi.digitech.file.processing.reader.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import com.adp.esi.digitech.file.processing.ds.config.model.FileMetaData;
import com.adp.esi.digitech.file.processing.ds.model.ColumnRelation;
import com.adp.esi.digitech.file.processing.exception.MetadataValidationException;
import com.adp.esi.digitech.file.processing.model.ErrorData;
import com.adp.esi.digitech.file.processing.util.ValidationUtil;

public abstract class AbstractCSVReaderService<T,V> extends AbstractReaderService<T,V> {
	
	
	public  String getValue(CSVRecord record, ColumnRelation columnRelation, String processing) {
		
		if(HEADER.equalsIgnoreCase(processing))
			return record.isMapped(columnRelation.getColumnName()) ?  record.get(columnRelation.getColumnName()) : null;
		
		//return record.get((int)(columnRelation.getPosition()-1));
		return getValue(record.values(), columnRelation.getPosition());
	}
	
	public String getValue(String[] dataArray, long position) {
		
			return dataArray.length >= position ? dataArray[(int)(position-1)] : null;
	}
	
	
	public CSVFormat newCsvFormat(FileMetaData fileMetaData, BufferedReader reader) throws IOException {
		var builder = CSVFormat.Builder.create(CSVFormat.EXCEL).setDelimiter(fileMetaData.getDelimeter());
		
		//header/position
		if(HEADER.equalsIgnoreCase(fileMetaData.getProcessing())) {
			for (int i = 0; i < fileMetaData.getHeaderIndex(); i++) {
				reader.readLine();
			}
			
			String headerLine = reader.readLine();
			if(!ValidationUtil.isHavingValue(headerLine)) {
				var metadataValidationException = new MetadataValidationException("Headers Validation Failed");
				metadataValidationException.setRequestContext(requestContext);
				metadataValidationException.setErrors(List.of(new ErrorData(fileMetaData.getSourceKey(), "CSV Parsing failed, reason = No Header found to process")));
				throw metadataValidationException;
			}
			
			var headers = headerLine.split(fileMetaData.getDelimeter());				
			builder.setHeader(headers).setSkipHeaderRecord(false);				
		}
		
		return builder.build();
	}
	
	public void validate(FileMetaData fileMetaData, List<ColumnRelation> columnRelations, List<String> reqHeaders) throws MetadataValidationException {
		var dbHeaders = columnRelations.stream().map(ColumnRelation::getColumnName).toList();
		this.validate(fileMetaData.getSourceKey(), dbHeaders, reqHeaders);
	}
	
	public void validate(FileMetaData fileMetaData, List<ColumnRelation> columnRelations, CSVRecord record) throws MetadataValidationException {
		var strArray = record.values();
		this.validate(fileMetaData.getSourceKey(), columnRelations.size(), strArray.length);
	}
}
