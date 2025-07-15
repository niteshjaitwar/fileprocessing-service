package com.adp.esi.digitech.file.processing.util;

import java.io.FileNotFoundException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.adp.esi.digitech.file.processing.model.Row;

@Service
public class FileUtils {
	
	@Autowired Environment environment;
	
	public String constructFileName(String uniqueId, JSONObject fileNamejson, Row row) {  
		//String transformRequired = ValidationUtil.isHavingValue(transformRequired) ? transformRequired : "Y";
		var isTransformRequired = (fileNamejson.has("isTransformationRequired") && !fileNamejson.isNull("isTransformationRequired")) ? fileNamejson.getString("isTransformationRequired") : "Y";
				
        ArrayList<String> coldata = new ArrayList<String>();
        if(ValidationUtil.isHavingValue(uniqueId))
        	coldata.add(uniqueId);
        if (fileNamejson.has("columns") && !fileNamejson.isNull("columns") && row != null) {
            JSONArray columns = fileNamejson.getJSONArray("columns");
            
            for(int j=0; j < columns.length(); j++) { 
            	var uuid = UUID.fromString(columns.getString(j));
                var dataObj =  isTransformRequired.equalsIgnoreCase("Y") ? row.getColumns().get(uuid).getTargetValue() : row.getColumns().get(uuid).getSourceValue();            
                coldata.add(String.valueOf(dataObj));
            }  
        }
            
        if (fileNamejson.has("static name") && !fileNamejson.isNull("static name")) {
        	coldata.add(fileNamejson.getString("static name"));
        }
        
        if (fileNamejson.has("suffix") && !fileNamejson.isNull("suffix") && row != null) {
        	JSONArray suffixes = fileNamejson.getJSONArray("suffix");
            for(int j=0;j<suffixes.length();j++) {
            	var uuid = UUID.fromString(suffixes.getString(j));
            	var dataObj = isTransformRequired.equalsIgnoreCase("Y") ? row.getColumns().get(uuid).getTargetValue() : row.getColumns().get(uuid).getSourceValue();            
                coldata.add(String.valueOf(dataObj));
            }    
        }
        var seperator = fileNamejson.has("seperator") && !fileNamejson.isNull("seperator") ? fileNamejson.getString("seperator") : "";
        return coldata.stream().collect(Collectors.joining(seperator)).toString();
	}
	
	public String getTargetFolderPath(String targetFolderPath, String appCode) {
		String sharedDriveServer = environment.getProperty(appCode +".shared.drive.server");
		return sharedDriveServer + targetFolderPath;
	}
	
	public String getFileName(String fileName) {
		
		if(!ValidationUtil.isHavingValue(fileName)) {
			throw new IllegalArgumentException("fileName can't be null");
		}
		
		return fileName.substring(0, fileName.lastIndexOf("."));
	}
	
	public String getFileExtension(String fileName) {
		
		if(!ValidationUtil.isHavingValue(fileName)) {
			throw new IllegalArgumentException("fileName can't be null");
		}
		
		return fileName.substring(fileName.lastIndexOf(".") + 1);
	}
	
	public MultipartFile constructMultipartFile(ResponseEntity<byte[]> response, String fileLocation)
			throws FileNotFoundException {
		try {
			if (Objects.isNull(response) || Objects.isNull(response.getBody()))
				throw new FileNotFoundException("response can't be empty");

			if (!response.getStatusCode().is2xxSuccessful())
				throw new FileNotFoundException("Can't find file for the given path " + fileLocation);

			String fileName = URLDecoder.decode(response.getHeaders().getContentDisposition().getFilename(), "UTF-8");
			return new CustomMultipartFile(response.getBody(), fileName, this.getContentType(fileName));

		} catch (Exception e) {
			throw new FileNotFoundException("Failed to construct multipart file" + ", message = " + e.getMessage());
		}
	}
	
	public String getContentType(String fileName) {
		String type = fileName.substring(fileName.lastIndexOf(".") + 1);
		
		String mimeType="";
		
		switch(type.toLowerCase())
		{
		case "pdf":
			mimeType="application/pdf";
			break;
		case "doc":
			mimeType="application/msword";
			break;
		case "docx":
			mimeType="application/vnd.openxmlformats-officedocument.wordprocessingml.document";
			break;
		case "xlsx":
			mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
			break;
		case "tif":
			mimeType="image/tiff";
			break;
		case "tiff":
			mimeType="image/tiff";
			break;
		case "jpg":
			mimeType="image/jpeg";
			break;
		case "jpeg":
			mimeType="image/jpeg";
			break;
		case "htm":
			mimeType="text/html";
			break;
		case "html":
			mimeType="text/html";
			break;
		case "gif":
			mimeType="image/gif";
			break;
		case "png":
			mimeType="image/png";
			break;
		case "txt":
			mimeType="text/plain";
			break;
		case "json":
			mimeType="application/json";
			break;
		case "sig":
			mimeType="application/pgp-signature";
			break;
		default:
			mimeType="application/octet-stream";
			break;	
		}
		
		return mimeType;
	}
}
