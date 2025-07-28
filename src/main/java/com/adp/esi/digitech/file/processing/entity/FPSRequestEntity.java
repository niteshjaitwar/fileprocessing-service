package com.adp.esi.digitech.file.processing.entity;

import java.util.Date;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity(name = "MS_FPS_REQUEST")
@Table(name = "MS_FPS_REQUEST")
@Data
@AllArgsConstructor
@NoArgsConstructor
public class FPSRequestEntity {

	@Id
	@Column(name = "ID")
	@GeneratedValue(generator = "SEQ_MS_FPS_REQUEST", strategy = GenerationType.SEQUENCE)
	@SequenceGenerator(name = "SEQ_MS_FPS_REQUEST", sequenceName = "SEQ_MS_FPS_REQUEST", allocationSize = 1)
	private Long requestId;

	@Column(name = "UNIQUE_ID")
	private String uniqueId;

	@Column(name = "UUID")
	private String uuid;

	@Column(name = "BU")
	private String bu;

	@Column(name = "PLATFORM")
	private String platform;

	@Column(name = "DATA_CATEGORY")
	private String dataCategory;

	@Column(name = "SAVE_FILE_LOCATION")
	private String saveFileLocation;

	@Column(name = "SOURCE_TYPE")
	private String sourceType;

	@Column(name = "REQUEST_PAYLOAD")
	@Lob
	private String requestPayload;// clob

	@Column(name = "STATUS")
	private String status;

	@Column(name = "ERROR_TYPE")
	private String errorType;

	@Column(name = "ERROR_DETAILS")
	@Lob
	private String errorDetails;// clob

	@Column(name = "CREATED_BY")
	private String createdBy;

	@Column(name = "CREATED_DATE_TIME")
	private Date createdDate;

	@Column(name = "MODIFIED_BY")
	private String modifiedBy;

	@Column(name = "MODIFIED_DATE_TIME")
	private Date modifiedDate;

}
