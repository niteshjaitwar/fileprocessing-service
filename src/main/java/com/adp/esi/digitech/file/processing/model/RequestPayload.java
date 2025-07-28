package com.adp.esi.digitech.file.processing.model;

import java.util.List;

import org.apache.poi.ss.usermodel.Workbook;
import org.json.JSONObject;
import org.springframework.web.multipart.MultipartFile;

import com.adp.esi.digitech.file.processing.json.deserializer.FormTextDeserializer;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class RequestPayload {
	
	private String bu; 
	
	private String platform; 
	
	private String dataCategory; 
	
	private String subDataCategory;
	
	private String uniqueId;
	
	private String saveFileLocation;
	
	@JsonProperty("ecosystem_id")
	private String ecosystemId;
	
	private String useremail;
	
	private String source;
	
	@JsonProperty("form_data")
	private String formData;
	
	private JSONObject jsonPayload;
	
	@JsonProperty("form_text")
	@JsonDeserialize(using = FormTextDeserializer.class)
	private JSONObject rawJsonPayload;
	
	private Workbook excelFile;
	
	private int noOfRowsInexcelFile;
	
	private MultipartFile file;
	
	@NotEmpty(message = "documents are Required")
	private List<@Valid Document> documents;
}
