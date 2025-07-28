package com.adp.esi.digitech.file.processing.generator.document.element;

import com.adp.esi.digitech.file.processing.generator.document.config.model.Element;
import com.adp.esi.digitech.file.processing.model.DataSet;
import com.adp.esi.digitech.file.processing.model.Row;

public interface IElementBuilder<T> {

	public void build(T doc, Element element, DataSet<Row> data );
	public com.lowagie.text.Element getElement(Element element, DataSet<Row> data );
}
