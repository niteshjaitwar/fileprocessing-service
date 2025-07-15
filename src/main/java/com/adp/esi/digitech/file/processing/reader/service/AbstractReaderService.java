package com.adp.esi.digitech.file.processing.reader.service;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.adp.esi.digitech.file.processing.autowire.service.CustomValidatorDynamicAutowireService;
import com.adp.esi.digitech.file.processing.ds.model.ColumnRelation;
import com.adp.esi.digitech.file.processing.exception.MetadataValidationException;
import com.adp.esi.digitech.file.processing.exception.ReaderException;
import com.adp.esi.digitech.file.processing.model.Column;
import com.adp.esi.digitech.file.processing.model.ErrorData;
import com.adp.esi.digitech.file.processing.model.Metadata;
import com.adp.esi.digitech.file.processing.model.RequestContext;
import com.adp.esi.digitech.file.processing.validation.service.HeaderValidationService;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractReaderService<T,V> implements IReaderService<T,V> {
	
	public RequestContext requestContext;
	
	public ObjectProvider<Column> columnObjProvider;
	
	public CustomValidatorDynamicAutowireService validatorDynamicAutowireService;
	
	public ObjectMapper objectMapper;
	
	public Executor asyncExecutor;
	
	@Value("${large.request.file.path}")
	protected String largeRequestFilePath;
	
	public static final String HEADER = "header";
	public static final String POSITION = "position";

	@Override
	public void setRequestContext(RequestContext requestContext) {
		this.requestContext = requestContext;
	}

	@Autowired
	public void setColumnObjProvider(ObjectProvider<Column> columnObjProvider) {
		this.columnObjProvider = columnObjProvider;
	}
	
	@Autowired
	protected void setValidatorDynamicAutowireService(
			CustomValidatorDynamicAutowireService validatorDynamicAutowireService) {
		this.validatorDynamicAutowireService = validatorDynamicAutowireService;
	}

	@Autowired
	protected void setObjectMapper(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	@Autowired
	protected void setAsyncExecutor(Executor asyncExecutor) {
		this.asyncExecutor = asyncExecutor;
	}

	public static String getName(String sourceKey) {
		return sourceKey.replace(" " , "_").replace("{{", "$").replace("}}", "").trim();
	}
	
	
	public void validate(String sourceKey, int dbHeaders, int reqHeaders) {
	    if (reqHeaders != dbHeaders) {
	        throw createMetadataValidationException(sourceKey, "There is a mismatch between column configured and data");
	    }
	}

	public void validate(String sourceKey, List<String> dbHeaders, List<String> reqHeaders) throws MetadataValidationException {
	    try {
	        validatorDynamicAutowireService.validate(HeaderValidationService.class, new Metadata(reqHeaders, dbHeaders), this.requestContext);
	    } catch (MetadataValidationException e) {
	        throw createMetadataValidationException(sourceKey, e.getMessage());
	    }
	}
	
	public MetadataValidationException createMetadataValidationException(String sourceKey, String message) {
	    var exception = new MetadataValidationException("Headers Validation Failed");
	    exception.setRequestContext(requestContext);
	    exception.setErrors(List.of(new ErrorData(sourceKey, message)));
	    return exception;
	}
	
	public MetadataValidationException createMetadataValidationException(List<ErrorData> errors) {
	    var exception = new MetadataValidationException("Headers Validation Failed");
	    exception.setRequestContext(requestContext);
	    exception.setErrors(errors);
	    return exception;
	}
	
	public Map<String, SimpleDateFormat> getDateFormats(Stream<ColumnRelation> columnRelations) {
		return columnRelations.filter(cr -> "DATE".equalsIgnoreCase(cr.getDataType()))
				.map(ColumnRelation::getFormat).distinct()
				.collect(Collectors.toMap(Function.identity(), SimpleDateFormat::new));
	}
	
	protected <K> void write(Path dir, String fileName, Map<String, List<K>> data) throws ReaderException {
		try {
			var file = Paths.get(dir.toString(), fileName + ".json").toFile();
			objectMapper.writeValue(file, data);
		} catch (IOException e) {
			log.error("Failed to write file {} in directory {}, message = {}", fileName, dir, e.getMessage());
			throw new ReaderException("Failed to write batch file: " + e.getMessage(), e);
		}
	}
}
