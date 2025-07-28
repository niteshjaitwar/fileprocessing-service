package com.adp.esi.digitech.file.processing.generator.document.config.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

//@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonTypeInfo( use = JsonTypeInfo.Id.DEDUCTION )
@JsonSubTypes({
	@JsonSubTypes.Type(value = Image.class, name = "image"),
	@JsonSubTypes.Type(value = Paragraph.class, name = "paragraph"),
	@JsonSubTypes.Type(value = ElmTable.class, name = "table"),
	@JsonSubTypes.Type(value = Shape.class, name = "shape"),
	@JsonSubTypes.Type(value = Break.class, name = "break"),
	@JsonSubTypes.Type(value = HDList.class, name = "list")
})
public abstract class Component {

}
