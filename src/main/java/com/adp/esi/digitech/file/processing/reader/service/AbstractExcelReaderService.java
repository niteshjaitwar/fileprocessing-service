package com.adp.esi.digitech.file.processing.reader.service;

import java.util.List;
import java.util.stream.Collectors;

import com.adp.esi.digitech.file.processing.ds.config.model.FileMetaData;
import com.adp.esi.digitech.file.processing.exception.MetadataValidationException;
import com.adp.esi.digitech.file.processing.model.ErrorData;

public abstract class AbstractExcelReaderService<T,V> extends AbstractReaderService<T,V> {
	
	
	public MetadataValidationException createMetadataValidationExceptionSheet(List<FileMetaData> invalidFileMetadataList) {
		var exception = new MetadataValidationException("Invalid Excel File, Missing sheet data");
	    exception.setRequestContext(requestContext);
	    var errors = invalidFileMetadataList.stream().map(invalidFileMetadata -> {
	    	var key = invalidFileMetadata.getSourceKey();
			var sheetName = key.contains("{{") && key.endsWith("}}") ? key.substring(key.indexOf("{{") + 2, key.indexOf("}}")) : key;
	    	return new ErrorData(invalidFileMetadata.getSourceKey(), "Missing " + sheetName + " sheet data");
	    	
	    }).collect(Collectors.toList());
	    exception.setErrors(errors);
	    throw exception;
	}

}
