package com.adp.esi.digitech.file.processing.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ChunkPosition {
	
	private String fileName;

	private long startPosition;
	
	private long endPosition;
}
