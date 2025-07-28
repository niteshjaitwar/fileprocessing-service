package com.adp.esi.digitech.file.processing.util;

import java.io.File;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import lombok.extern.slf4j.Slf4j;


@Component
@Slf4j
public class FileValidationUtils {
	
	@Value("${file.small.maxSizeAllowed}")
	private long maxSmallFileSize;	
	
	@Value("${file.large.maxSizeAllowed}")
	private long maxLargeFileSize;	
	
	@Autowired
	private FileUtils fileUtils;
	
	
	private boolean isAllowedSmallFileSize(long fileSize) {
		log.info("small file max = {}, actual={}", maxSmallFileSize, fileSize);
		return fileSize < maxSmallFileSize;
	}
	
	private boolean isAllowedLargeFileSize(long fileSize) {
		log.info("large file max = {}, actual={}", maxLargeFileSize, fileSize);
		return fileSize < maxLargeFileSize;
	}
	
	public boolean validate(MultipartFile file, String fileType) {
		return !(isFormat(file.getOriginalFilename(), fileType) && isAllowedSmallFileSize(file.getSize()));
	}
	
	public boolean validate(String filePath, String fileType) {
		File file = new File(filePath);
		return !(isFormat(file.getName(), fileType) && isAllowedLargeFileSize(file.length()));
	}
	
	private boolean isFormat(String filename, String fileType) {
		//excelFileType.equalsIgnoreCase(fileType);
		log.info("format validate = {}", fileUtils.getFileExtension(filename).equalsIgnoreCase(fileType));
		return fileUtils.getFileExtension(filename).equalsIgnoreCase(fileType);
	}

}
