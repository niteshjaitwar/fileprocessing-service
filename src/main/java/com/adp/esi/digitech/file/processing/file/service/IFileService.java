package com.adp.esi.digitech.file.processing.file.service;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.springframework.http.ResponseEntity;
import org.springframework.web.multipart.MultipartFile;

import com.adp.esi.digitech.file.processing.file.dto.SharedFileResponseDTO;
import com.adp.esi.digitech.file.processing.model.RequestContext;
import com.adp.esi.digitech.file.processing.model.SharedFile;
import com.fasterxml.jackson.core.JsonProcessingException;

public interface IFileService {
	
	public void setRequestContext(RequestContext requestContext);
	
	public MultipartFile getFile(String fileLocation, String appCode) throws FileNotFoundException, JsonProcessingException;
	
	public ResponseEntity<byte[]> getFileParts(String fileLocation, String appCode) throws FileNotFoundException, JsonProcessingException;

	public boolean isFileExists(String fileLocation, String appCode);
	
	public SharedFileResponseDTO uploadFile(SharedFile sharedFile);

	public SharedFileResponseDTO uploadFile(SharedFile sharedFile, String uploadFileUrl);
	
	public void copyToLocal(String folder, String fileName, MultipartFile sharedFile) throws IOException;
	
	public void deleteFromLocal(String path) throws IOException;
	
	public SharedFileResponseDTO deleteFile(String fileLocation,String uniqueId, String appCode);

}
