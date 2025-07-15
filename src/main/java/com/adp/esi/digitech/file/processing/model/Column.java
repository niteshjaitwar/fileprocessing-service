package com.adp.esi.digitech.file.processing.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.UUID;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Column implements Cloneable, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 7844510387316842970L;
	private String name;
	private Object value;	
	private Object transformedValue;
	private UUID uuid;
	private String sourceKey;
	private String targetName;
	private String targetValue;
	private String sourceValue;
	//private int rowIndex;
	
	private ArrayList<String> errors;
	
	public Column(String name, Object value, UUID uuid, String sourceKey) {
		this.name = name;
		this.value = value;
		this.uuid = uuid;
		this.sourceKey = sourceKey;
	}
	
	@Override
	public Object clone() {
		Column column = null;
	    
	    try {
	        column = (Column) super.clone();
	        
	    } catch (CloneNotSupportedException e) {
	        column = new Column(this.name, this.value,this.uuid,this.sourceKey);
	        column.setSourceValue(this.sourceValue);
	        column.setTargetValue(this.targetValue);
	        column.setTransformedValue(this.transformedValue);
	        //column.setRowIndex(this.rowIndex);
	    }
	    
	    return column;
	}
		
}
