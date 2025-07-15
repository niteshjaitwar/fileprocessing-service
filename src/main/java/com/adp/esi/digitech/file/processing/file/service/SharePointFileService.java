package com.adp.esi.digitech.file.processing.file.service;

import java.io.FileNotFoundException;
import java.net.URI;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.util.UriComponentsBuilder;

import com.adp.esi.digitech.file.processing.enums.Status;
import com.adp.esi.digitech.file.processing.file.dto.SharedFileResponseDTO;
import com.adp.esi.digitech.file.processing.file.dto.SharepointRequestDTO;
import com.adp.esi.digitech.file.processing.file.dto.SharepointResponseDTO;
import com.adp.esi.digitech.file.processing.model.SharedFile;
import com.fasterxml.jackson.core.JsonProcessingException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
//@Service
//@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SharePointFileService extends AbstractFileService {

	@Value("${ibpm.sharepoint.file.server.getFileByLocation}")
	private String getFileByLocationUrl;

	@Value("${ibpm.sharepoint.file.server.isFileExists}")
	private String isFileExistsUrl;

	@Value("${ibpm.sharepoint.file.server.uploadFile}")
	private String uploadFileUrl;

	@Override
	public MultipartFile getFile(String fileLocation, String appCode) throws FileNotFoundException, JsonProcessingException {
		return fileUtils.constructMultipartFile(getFileParts(fileLocation, appCode), fileLocation);
	}

	@Override
	public ResponseEntity<byte[]> getFileParts(String fileLocation, String appCode) throws FileNotFoundException, JsonProcessingException {
		log.info("SharePointFilesService-getFileParts - Calling REST to get file, UniqueId = {}, fileLocation = {}",	requestContext.getUniqueId(), fileLocation);
		RestTemplate rt = new RestTemplate();
		UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(getFileByLocationUrl);
		URI uri = builder.build().toUri();
		log.info("SharePointFilesService-getFileParts - Calling REST to get file, UniqueId = {},  URI Parameters = {}",requestContext.getUniqueId(), uri);
		var fileName = fileLocation.substring(fileLocation.lastIndexOf("/")+1, fileLocation.length());
		fileLocation = fileLocation.substring(0, fileLocation.lastIndexOf("/") + 1);

		SharepointRequestDTO body = SharepointRequestDTO.builder().fileLocation(fileLocation).fileName(fileName).trackingId(requestContext.getUniqueId()).build();
		
		log.info("SharePointFilesService-getFileParts - Calling REST to get file, UniqueId = {},  body = {}",requestContext.getUniqueId(), body);
		
		ResponseEntity<byte[]> response = rt.exchange(uri, HttpMethod.POST, getEntity(appCode, body, MediaType.APPLICATION_JSON), byte[].class);
		log.info("SharePointFilesService-getFile - response of file, UniqueId = {},  success = {}",	requestContext.getUniqueId(), response.getStatusCode().is2xxSuccessful());
		return response;
	}

	@Override
	public boolean isFileExists(String fileLocation, String appCode) {
		try {
			log.info("SharedFileService-isFileExists - Calling REST to find file, UniqueId = {},  fileLocation = {}",
					requestContext.getUniqueId(), fileLocation);
			RestTemplate rt = new RestTemplate();
			UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(this.isFileExistsUrl);
			URI uri = builder.build().toUri();
			log.info("SharedFileService-isFileExists - Calling REST to find file, UniqueId = {},  URI Parameters = {}",	requestContext.getUniqueId(), uri);
			var fileName = fileLocation.substring(fileLocation.lastIndexOf("/")+1, fileLocation.length());
			fileLocation = fileLocation.substring(0, fileLocation.lastIndexOf("/") + 1);

			SharepointRequestDTO body = SharepointRequestDTO.builder().fileLocation(fileLocation).fileName(fileName)
					.trackingId(requestContext.getUniqueId()).build();

			log.info("SharePointFilesService-isFileExists - Calling REST to find file, UniqueId = {},  body = {}",requestContext.getUniqueId(), body);
			
			ResponseEntity<SharepointResponseDTO> response = rt.exchange(uri, HttpMethod.POST,
					getEntity(appCode, body, MediaType.APPLICATION_JSON), SharepointResponseDTO.class);

			log.info(
					"SharedFileService-isFileExists - response of find file, UniqueId = {}, URI Parameters = {}, response = {}", requestContext.getUniqueId(), uri, response.getBody());
			var optinalBody = Optional.ofNullable(response.getBody());

			if (optinalBody.isPresent())
				return optinalBody.get().getStatus().equals(Status.SUCCESS.getStatus());

		} catch (Exception e) {
			log.error("SharedFileService-isFileExists - Catch Error, uniqueId = {}, fileLocation = {}, error = {}",
					requestContext.getUniqueId(), fileLocation, e.getMessage());
		}
		return false;
	}

	@Override
	public SharedFileResponseDTO uploadFile(SharedFile sharedFile) {
		return uploadFile(sharedFile, this.uploadFileUrl);
	}
}
