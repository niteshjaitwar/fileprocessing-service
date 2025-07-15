package com.adp.esi.digitech.file.processing.ds.dto;

import com.adp.esi.digitech.file.processing.enums.Status;

import lombok.Data;

@Data
public class DSResponseDTO<T> {
	
	private Status status;
	private Object error;
	private T data;
	private String message;
}
