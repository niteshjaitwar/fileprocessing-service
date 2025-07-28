package com.adp.esi.digitech.file.processing.processor.service;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.adp.esi.digitech.file.processing.enums.ProcessType;
import com.adp.esi.digitech.file.processing.exception.ConfigurationException;
import com.adp.esi.digitech.file.processing.exception.DataValidationException;
import com.adp.esi.digitech.file.processing.exception.GenerationException;
import com.adp.esi.digitech.file.processing.exception.ProcessException;
import com.adp.esi.digitech.file.processing.exception.ReaderException;
import com.adp.esi.digitech.file.processing.exception.TransformationException;
import com.adp.esi.digitech.file.processing.exception.ValidationException;
import com.adp.esi.digitech.file.processing.model.DataMap;
import com.adp.esi.digitech.file.processing.model.DataSet;
import com.adp.esi.digitech.file.processing.model.RequestContext;
import com.adp.esi.digitech.file.processing.model.RequestPayload;
import com.adp.esi.digitech.file.processing.model.Row;
import com.adp.esi.digitech.file.processing.model.SharedFile;


public interface IProcessorService<T> {
	
	void setRequestContext(RequestContext requestContext);
	
	void setFileProcessType(ProcessType processType);
	
	T process(RequestPayload request) throws IOException, ReaderException, ConfigurationException, ReaderException, ValidationException, TransformationException, GenerationException, ProcessException;
	
	//Map<String, List<DataMap>> read(RequestPayload request) throws ReaderException;
	
	//void validate(RequestPayload request) throws ValidationException;
	
	//void validate(List<DataSet<DataMap>> dataSets) throws DataValidationException;
	
	//Map<String, DataSet<Row>> transfrom(List<DataSet<DataMap>> dataSets) throws TransformationException;
	
	//List<SharedFile> generate(Map<String, DataSet<Row>> dataSetsMap, String outputRules) throws GenerationException;
	
	//void send(List<SharedFile> sharedFiles) throws ProcessException;
	
	//void sendAsync(List<SharedFile> sharedFiles);

}
