package com.adp.esi.digitech.file.processing.file.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SharepointResponseDTO {
	
	private String status;
	
	private String message;
	
	private Object error;
	
	private Object data;
	
}
