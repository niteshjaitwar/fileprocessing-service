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
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity(name = "MS_FPS_REQUEST_QUEUE")
@Table(name = "MS_FPS_REQUEST_QUEUE")
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class FPSRequestQueueEntity {
	@Id
	@Column(name = "ID")
	@GeneratedValue(generator = "SEQ_MS_FPS_REQUEST_QUEUE", strategy = GenerationType.SEQUENCE)
	@SequenceGenerator(name = "SEQ_MS_FPS_REQUEST_QUEUE", sequenceName = "SEQ_MS_FPS_REQUEST_QUEUE", allocationSize = 1)
	private Long id;

	@Column(name = "UNIQUE_ID")
	private String uniqueId;

	@Column(name = "UUID")
	private String uuid;
	
	@Column(name = "STATUS", nullable = false)
	private String status;
	
	@Column(name = "CREATED_DATE_TIME", nullable = false, updatable = false)
	private Date createdDate;
	
	@Column(name = "MODIFIED_DATE_TIME")
	private Date modifiedDate;
	
	@Column(name = "REQUEST_PAYLOAD")
	@Lob
	private String requestPayload;
}
