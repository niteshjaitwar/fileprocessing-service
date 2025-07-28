package com.adp.esi.digitech.file.processing.generator.document;

import java.util.List;
import java.util.Map;

import com.adp.esi.digitech.file.processing.generator.document.config.model.MetaData;
import com.adp.esi.digitech.file.processing.generator.document.config.model.Page;
import com.adp.esi.digitech.file.processing.model.DataSet;
import com.adp.esi.digitech.file.processing.model.Row;

public interface IDocumentBuilder<T> {
	
	public T newInstance();
	
	public void setPages(T doc, List<Page> pages, Map<String,DataSet<Row>> dataSets);
	
	
	
}
