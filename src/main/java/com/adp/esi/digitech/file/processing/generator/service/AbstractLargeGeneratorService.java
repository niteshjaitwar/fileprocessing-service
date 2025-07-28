package com.adp.esi.digitech.file.processing.generator.service;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.adp.esi.digitech.file.processing.model.Row;
import com.adp.esi.digitech.file.processing.util.FileUtils;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractLargeGeneratorService<T,R> extends AbstractGeneratorService<T,R> {
	
	@Value("${large.request.file.path}")
	String largeRequestFilePath;
	
	@Autowired
	ObjectMapper objectMapper;
	
	@Autowired
	JsonFactory jsonFactory;
	
	@Autowired
	FileUtils fileUtils;
	
	public String constructFileName(JSONObject outputFileRule) throws IOException{
		Row[] tempRow = {null};
		var fileNameJson = outputFileRule.getJSONObject("fileName");
		if(outputFileRule.has("dataSetName") && !outputFileRule.isNull("dataSetName")) {				
			if (fileNameJson.has("columns") && !fileNameJson.isNull("columns")) {
				var dataSetId = outputFileRule.getString("dataSetName");
				var requestDir = largeRequestFilePath + requestContext.getRequestUuid();
				var dataSetDir = requestDir + "/datasets";
				var dir = Paths.get(dataSetDir + "/" + dataSetId + "/transform");
				
				Files.walk(dir).filter(path -> path.toFile().isFile()).findFirst()
				.ifPresent(path -> {
					try {
						var sReader = new FileReader(path.toFile());
						var rows = objectMapper.readValue(sReader, new TypeReference<List<Row>>() {});
						if(Objects.nonNull(rows) && !rows.isEmpty()) {
							tempRow[0] = rows.get(0);
						}
					} catch(IOException e) {
						log.error("AbstractLargeGeneratorService -> generate() Failed to load temp row for file name creation, uniqueId = {}, message = {}", requestContext.getUniqueId(), e.getMessage());
					}
				});
			}
		}
		
		
		return fileUtils.constructFileName(requestContext.getUniqueId(), fileNameJson, tempRow[0]);
	}
	
	public Path getOutputPath(String  fileName, String extension) throws IOException{
		
		var outputDir = Paths.get(largeRequestFilePath + requestContext.getRequestUuid() + "/output");
					
		if(Files.notExists(outputDir)) {
			Files.createDirectories(outputDir);
		}
					
		return Paths.get(outputDir + "/" + fileName + "." + extension);
	}

}
