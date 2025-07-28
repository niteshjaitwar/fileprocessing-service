package com.adp.esi.digitech.file.processing.generator.document;

import com.adp.esi.digitech.file.processing.generator.document.config.model.HeaderFooter;
import com.adp.esi.digitech.file.processing.generator.document.config.model.Page;
import com.adp.esi.digitech.file.processing.model.DataSet;
import com.adp.esi.digitech.file.processing.model.Row;

public interface IPageBuilder<T> {
	
	public void newInstance(T doc, Page page);
	
//	public void setHeader(T doc, HeaderFooter header);
//	
//	public void setFooter(T doc, HeaderFooter footer);
	
	public void setHeaderFooter(T doc, HeaderFooter header,HeaderFooter footer);
	
	public void setElements(T doc, Page page, DataSet<Row> data );
}
