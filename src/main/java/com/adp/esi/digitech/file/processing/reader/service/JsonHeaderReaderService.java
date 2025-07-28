package com.adp.esi.digitech.file.processing.reader.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.map.HashedMap;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;


import com.adp.esi.digitech.file.processing.exception.ReaderException;

import lombok.extern.slf4j.Slf4j;

@Service("jsonHeaderReaderService")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class JsonHeaderReaderService extends AbstractReaderService<String,JSONObject> {
	
	@Autowired
	ObjectProvider<JsonReaderService> objectProviderJsonReaderService;
	
	@Override
	public Map<String, List<String>> read(JSONObject data) throws ReaderException {
		log.info("JsonHeaderReaderService -> read() Started processing JSONObject, uniqueId = {}", requestContext.getRequestUuid());
		
		JSONObject rawPayload = (JSONObject) data;
		var jsonReaderService = objectProviderJsonReaderService.getObject();
		jsonReaderService.setRequestContext(this.requestContext);				
		JSONArray payloadArray = jsonReaderService.processJson(rawPayload);
		
		Map<String, List<String>> headerInfo = new HashedMap<>();
		
		for(Object obj : payloadArray) {
			JSONObject payload = (JSONObject) obj;	
			JSONArray headers = payload.getJSONArray("headers");
			List<String> reqHeaders = new ArrayList<>();
			for (Object element : headers) {
				reqHeaders.add((String)element);
			}
			if(payload.has("sourceKey")) {
				headerInfo.put(payload.getString("sourceKey"), reqHeaders);
				payload.remove("sourceKey");
			} else {
				headerInfo.put(requestContext.getDataCategory(), reqHeaders);
			}
		}
		log.info("JsonHeaderReaderService -> read() Completed processing JSONObject, uniqueId = {}, headerInfo = {}", requestContext.getRequestUuid(), headerInfo);
		return headerInfo;
	}
}
