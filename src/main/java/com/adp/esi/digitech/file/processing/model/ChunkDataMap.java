package com.adp.esi.digitech.file.processing.model;

import java.io.Serializable;
import java.util.List;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class ChunkDataMap implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 677872786671903140L;
	
	private String groupIdentifierValue;
	private List<String> chunkLocations;
	private List<ChunkPosition> chunkPositions;
	
	public ChunkDataMap(String groupIdentifierValue, List<String> chunkLocations) {
		
		this.groupIdentifierValue = groupIdentifierValue;
		this.chunkLocations = chunkLocations;
	}

	public ChunkDataMap(String groupIdentifierValue, List<ChunkPosition> chunkPositions, boolean isChunkPositions) {
		this.groupIdentifierValue = groupIdentifierValue;
		this.chunkPositions = chunkPositions;
	}
	
	

}
