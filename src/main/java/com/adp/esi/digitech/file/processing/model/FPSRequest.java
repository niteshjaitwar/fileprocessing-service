package com.adp.esi.digitech.file.processing.model;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.Data;

@Data
public class FPSRequest {

	private Long requestId;

	private String uniqueId;

	private String uuid;

	private String bu;

	private String platform;

	private String dataCategory;

	private String saveFileLocation;

	private String sourceType;

	@JsonIgnore
	private String requestPayload;

	private String status;

	private String errorType;

	private String errorDetails;

	private String createdBy;

	private Date createdDate;

	private String modifiedBy;

	private Date modifiedDate;
}
