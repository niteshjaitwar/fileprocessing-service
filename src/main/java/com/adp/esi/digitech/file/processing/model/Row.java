package com.adp.esi.digitech.file.processing.model;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Row implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1546932450102031383L;
	private Map<UUID, Column> columns;

	public  Row(Map<UUID, Column> columns) {
		this.columns = columns;
	}
	
	
}
