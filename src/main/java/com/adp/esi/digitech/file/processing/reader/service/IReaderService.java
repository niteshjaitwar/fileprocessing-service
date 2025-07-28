package com.adp.esi.digitech.file.processing.reader.service;

import java.util.List;
import java.util.Map;

import com.adp.esi.digitech.file.processing.exception.ReaderException;
import com.adp.esi.digitech.file.processing.model.RequestContext;

public interface IReaderService<T,V> {
	
	public Map<String, List<T>> read(V data) throws ReaderException;
	
	public void setRequestContext(RequestContext requestContext);
}
