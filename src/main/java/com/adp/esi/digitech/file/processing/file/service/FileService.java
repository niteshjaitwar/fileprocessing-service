package com.adp.esi.digitech.file.processing.file.service;

import java.io.FileNotFoundException;

import org.springframework.http.ResponseEntity;
import org.springframework.web.multipart.MultipartFile;

import com.adp.esi.digitech.file.processing.file.dto.SharedFileResponseDTO;
import com.adp.esi.digitech.file.processing.model.SharedFile;
import com.fasterxml.jackson.core.JsonProcessingException;

public class FileService extends AbstractFileService {

	@Override
	public MultipartFile getFile(String fileLocation, String appCode)
			throws FileNotFoundException, JsonProcessingException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResponseEntity<byte[]> getFileParts(String fileLocation, String appCode)
			throws FileNotFoundException, JsonProcessingException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isFileExists(String fileLocation, String appCode) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public SharedFileResponseDTO uploadFile(SharedFile sharedFile) {
		// TODO Auto-generated method stub
		return null;
	}

}
