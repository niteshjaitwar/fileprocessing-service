package com.adp.esi.digitech.file.processing.file.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SharepointRequestDTO {

	private String fileLocation;
	private String fileName;
	private String destinationFileLocation;
	private String destinationFileName;
	private String trackingId;
	private String keepOriginal;
}
