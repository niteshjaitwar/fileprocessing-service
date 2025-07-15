package com.adp.esi.digitech.file.processing.file.service;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.util.UriComponentsBuilder;

import com.adp.esi.digitech.file.processing.enums.Status;
import com.adp.esi.digitech.file.processing.file.dto.SharedFileResponseDTO;
import com.adp.esi.digitech.file.processing.model.SharedFile;
import com.fasterxml.jackson.core.JsonProcessingException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
//@Service
//@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SharedDriveFileService extends AbstractFileService{
	
	@Value("${ibpm.file.server.getFileByLocation}")
	private String getFileByLocationUrl;
	
	@Value("${ibpm.file.server.isFileExists}")
	private String isFileExistsUrl;
	
	@Value("${ibpm.file.server.uploadFile}")
	private String uploadFileUrl;
	
	@Value("${ibpm.file.server.deleteFile}")
	private String deleteFileUrl;
	
	
	@Override
	public MultipartFile getFile(String fileLocation, String appCode) throws FileNotFoundException, JsonProcessingException {
		return fileUtils.constructMultipartFile(getFileParts(fileLocation, appCode), fileLocation);
	}
	
	@Override
	public ResponseEntity<byte[]> getFileParts(String fileLocation, String appCode) throws FileNotFoundException, JsonProcessingException {
		log.info("SharedDriveFileService-getFile - Calling REST to get file, UniqueId = {}, fileLocation = {}",	requestContext.getUniqueId(), fileLocation);
		URI uri = getURI(this.getFileByLocationUrl, fileLocation);
		log.info("SharedDriveFileService-getFile - Calling REST to get file, UniqueId = {},  URI Parameters = {}",requestContext.getUniqueId(), uri);
		RestTemplate rt = new RestTemplate();
		ResponseEntity<byte[]> response = rt.exchange(uri, HttpMethod.GET, getEntity(appCode), byte[].class);
		log.info("SharedDriveFileService-getFile - response of file, UniqueId = {},  success = {}",	requestContext.getUniqueId(), response.getStatusCode().is2xxSuccessful());
		return response;
	}
	
	@Override
	public boolean isFileExists(String fileLocation, String appCode) {
		try {
			log.info("SharedDriveFileService-isFileExists - Calling REST to find file, UniqueId = {},  fileLocation = {}",requestContext.getUniqueId(), fileLocation);
			URI uri = getURI(this.isFileExistsUrl, fileLocation);
			log.info("SharedDriveFileService-isFileExists - Calling REST to find file, UniqueId = {},  URI Parameters = {}",	requestContext.getUniqueId(), uri);
			RestTemplate rt = new RestTemplate();
			ResponseEntity<SharedFileResponseDTO> response = rt.exchange(uri, HttpMethod.GET, getEntity(appCode),
					SharedFileResponseDTO.class);
			log.info("SharedDriveFileService-isFileExists - response of find file, UniqueId = {}, URI Parameters = {}, response = {}",requestContext.getUniqueId(), uri, response.getBody());
			var optinalBody = Optional.ofNullable(response.getBody());

			if (optinalBody.isPresent())
				return optinalBody.get().getStatus().equalsIgnoreCase(Status.SUCCESS.getStatus());

		} catch (Exception e) {
			log.error("SharedDriveFileService-isFileExists - Catch Error, uniqueId = {}, fileLocation = {}, error = {}",
					requestContext.getUniqueId(), fileLocation, e.getMessage());

		}
		return false;
	}
	
	private URI getURI(String url, String fileLocation) {
		try {
			fileLocation = encode(fileLocation);
		} catch (UnsupportedEncodingException e) {
			log.error("SharedDriveFileService-getURI - Catch Error, UniqueId = {}, fileLocation = {}, error = {}",
					requestContext.getUniqueId(), fileLocation, e.getMessage());
		}
		UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(url).queryParam("fileLocation", fileLocation);
		return builder.build().toUri();
	}
	
	@Override
	public SharedFileResponseDTO uploadFile(SharedFile sharedFile) {
		return uploadFile(sharedFile, this.uploadFileUrl);
	}
	
	@Override
	public SharedFileResponseDTO deleteFile(String fileLocation,String uniqueId, String appCode) {
		try {
			log.info("SharedDriveFileService-deleteFile - Calling REST to delete file, uniqueId = {},  fileLocation = {}", uniqueId, fileLocation);
			RestTemplate rt = new RestTemplate();
			try {
				fileLocation = encode(fileLocation);
			} catch (UnsupportedEncodingException e) {
				log.error("SharedDriveFileService-deleteFile - Error, uniqueId = {},  fileLocation = {}, error = {}", uniqueId, fileLocation, e.getMessage());
			}
			UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(this.deleteFileUrl)
					.queryParam("fileLocation", fileLocation);
			URI uri = builder.build().toUri();
			log.info("SharedDriveFileService-deleteFile - Calling REST to delete file, uniqueId = {},  URI Parameters = {}", uniqueId, uri);
			ResponseEntity<SharedFileResponseDTO> response = rt.exchange(uri, HttpMethod.DELETE, getEntity(appCode), SharedFileResponseDTO.class);
			log.info("SharedDriveFileService-deleteFile - response of delete file, uniqueId = {}, fileLocation = {}, response = {}", uniqueId, fileLocation, response.getBody());
			return response.getBody();
		} catch (Exception e) {
			log.error("SharedDriveFileService-deleteFile - Error, uniqueId = {},  fileLocation = {}, error = {}", uniqueId, fileLocation, e.getMessage());
			SharedFileResponseDTO docResponse = new SharedFileResponseDTO();
			docResponse.setStatus(Status.FAILED.getStatus());
			docResponse.setReason(e.getMessage());
			return docResponse;
		}
	}
	
}
