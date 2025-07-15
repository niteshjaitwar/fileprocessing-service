package com.adp.esi.digitech.file.processing.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class KafkaPayload {	
	private String requestUuid;
	private RequestPayload payload;
}
