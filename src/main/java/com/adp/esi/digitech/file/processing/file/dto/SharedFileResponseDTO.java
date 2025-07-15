package com.adp.esi.digitech.file.processing.file.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class SharedFileResponseDTO {
	
	private String status;
	
	private String reason;
	
	private String message;
	
}
