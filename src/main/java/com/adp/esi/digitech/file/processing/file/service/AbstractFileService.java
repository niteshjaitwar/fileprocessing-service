package com.adp.esi.digitech.file.processing.file.service;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.util.UriComponentsBuilder;

import com.adp.esi.digitech.file.processing.enums.TargetLocation;
import com.adp.esi.digitech.file.processing.file.dto.SharedFileResponseDTO;
import com.adp.esi.digitech.file.processing.model.RequestContext;
import com.adp.esi.digitech.file.processing.model.SharedFile;
import com.adp.esi.digitech.file.processing.util.FileUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractFileService implements IFileService {

	RequestContext requestContext;

	@Autowired
	FileUtils fileUtils;

	@Autowired
	ObjectMapper objectMapper;

	public void setRequestContext(RequestContext requestContext) {
		this.requestContext = requestContext;
	}

	

	public String encode(String data) throws UnsupportedEncodingException {
		return URLEncoder.encode(data, "UTF-8");
	}

	public HttpEntity<?> getEntity(String appCode) throws JsonProcessingException {
		return getEntity(appCode, null);
	}

	public HttpEntity<?> getEntity(String appCode, Object body) throws JsonProcessingException {
		return getEntity(appCode, body, null);
	}

	public HttpEntity<?> getEntity(@NonNull String appCode, @Nullable Object body, @Nullable MediaType contentType)
			throws JsonProcessingException {
		if (Objects.nonNull(body)) {
			var bodyStr = objectMapper.writeValueAsString(body);
			return new HttpEntity<String>(bodyStr, constructHeaders(appCode, contentType));

		} else {
			return new HttpEntity<>(constructHeaders(appCode, contentType));
		}

	}

	private HttpHeaders constructHeaders(String appCode, @Nullable MediaType contentType) {
		HttpHeaders headers = new HttpHeaders();
		headers.add("app-code", appCode);
		if (Objects.nonNull(contentType))
			headers.setContentType(contentType);
		return headers;
	}

	@Override
	public void copyToLocal(String folderPath, String fileName, MultipartFile sharedFile) throws IOException {
		copyToLocal(folderPath, fileName, sharedFile.getBytes());
	}
	
	public void copyToLocal(String folderPath, String fileName, byte[] bytes) throws IOException {
		
		var fPath = Paths.get(folderPath);
		if(Files.notExists(fPath))
			Files.createDirectories(fPath);
		
		var root = "";
		String filePath = root + folderPath + "/" + fileName;
		
		var file = new File(filePath);		
		if(!file.exists())
			file.createNewFile();		
		Path path = Paths.get(filePath);
		Files.write(path, bytes);
	}

	@Override
	public void deleteFromLocal(String path) throws IOException {
		File file = new File(path);
		if (file.exists()) {
			var isDeleted = file.delete();
			if (!isDeleted) {
				log.error(
						"AbstractFilesService : deleteFromLocal : Failed to delete file from local, UniqueId = {},  location = {} ",
						requestContext.getUniqueId(), path);
			}
		}
	}

	@Override
	public SharedFileResponseDTO uploadFile(SharedFile sharedFile, String uploadFileUrl) {
		log.info("AbstractFilesService - uploadFile() UniqueId = {},  File Location {}, File Name {}",
				requestContext != null ? requestContext.getUniqueId() : "", sharedFile.getPath(),
				sharedFile.getName());
		try {
			ByteArrayResource contentsAsResource = new ByteArrayResource(sharedFile.getBytes()) {
				@Override
				public String getFilename() {
					return sharedFile.getName(); // Filename has to be returned in order to be able to post.
				}
			};

			UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(uploadFileUrl);
			URI uri = builder.build().toUri();

			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.MULTIPART_FORM_DATA);
			headers.add("app-code", sharedFile.getAppCode());
			headers.add("Content-disposition", "form-data; name=uploadfile; filename=" + sharedFile.getName());
			headers.add("Content-type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");

			log.info("Headers = {}", headers);
			MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();
			body.add("fileLocation", sharedFile.getPath());
			body.add("fileName", sharedFile.getName());
			body.add("file", contentsAsResource);

			if (sharedFile.getLocation().equals(TargetLocation.SharePoint))
				body.add("trackingId", requestContext != null ? requestContext.getUniqueId() : "");

			HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(body, headers);

			RestTemplate rt = new RestTemplate();

			log.info(
					"AbstractFilesService : uploadFile : Calling REST to upload file, UniqueId = {},  URI Parameters = {}, RequestEntity = {}, ",
					requestContext != null ? requestContext.getUniqueId() : "", uri, requestEntity.toString());
			ResponseEntity<SharedFileResponseDTO> response = rt.postForEntity(uri, requestEntity,
					SharedFileResponseDTO.class);

			var optinalBody = Optional.ofNullable(response.getBody());
			if (response.getStatusCode().is2xxSuccessful() && optinalBody.isPresent()
					&& optinalBody.get().getStatus().equalsIgnoreCase("SUCCESS")) {
				log.info(
						"AbstractFilesService : uploadFile : Completed saving file. UniqueId = {},  location = {} File Name {}, {}",
						requestContext != null ? requestContext.getUniqueId() : "", sharedFile.getPath(),
						sharedFile.getName(), response.getBody());
			} else {
				log.error(
						"AbstractFilesService : uploadFile : Fail to save file, UniqueId = {},  shared location {} File Name {}, response body: {}",
						requestContext != null ? requestContext.getUniqueId() : "", sharedFile.getPath(),
						sharedFile.getName(), response.getBody());
			}
			return response.getBody();

		} catch (Exception e) {
			log.error("AbstractFilesService : uploadFile : Error while uploading file, UniqueId = {},  via http = {}",
					requestContext != null ? requestContext.getUniqueId() : "", e.getMessage());
			return new SharedFileResponseDTO("Failed", "Error while uploading file, message = " + e.getMessage(), null);
		}
	}
	
	@Override
	public SharedFileResponseDTO deleteFile(String fileLocation, String instanceId, String appCode) {
		return null;
	}
}
