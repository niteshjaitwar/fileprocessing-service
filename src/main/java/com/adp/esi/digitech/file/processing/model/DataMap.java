package com.adp.esi.digitech.file.processing.model;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class DataMap implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -6054896180700107653L;
	
	private Map<UUID, String> columns;

	public  DataMap(Map<UUID, String> columns) {
		this.columns = columns;
	}

}
