package com.adp.esi.digitech.file.processing.reader.service;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.file.processing.ds.model.ColumnRelation;
import com.adp.esi.digitech.file.processing.exception.ReaderException;
import com.adp.esi.digitech.file.processing.model.DataMap;
import com.adp.esi.digitech.file.processing.util.ValidationUtil;

import lombok.extern.slf4j.Slf4j;


@Service("jsonReaderService")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class JsonReaderService extends AbstractReaderService<DataMap,JSONObject> {
	
	private Map<String, List<ColumnRelation>> columnRelationsMap;
	
	@Autowired(required = true)
	public JsonReaderService(Map<String, List<ColumnRelation>> columnRelationsMap) {
		this.columnRelationsMap = columnRelationsMap;
	}

	
	@Override
	public Map<String, List<DataMap>> read(JSONObject rawPayload) throws ReaderException {
		try {
			log.info("JsonReaderService -> read() Started processing JSONObject, uniqueId = {}", requestContext.getUniqueId());
			
			JSONArray payloadArray = processJson(rawPayload);			
			
			var rowsMap = IntStream.range(0, payloadArray.length()).parallel().mapToObj(index -> {
				JSONObject payload = payloadArray.getJSONObject(index);
				
				var sourceKey = payload.has("sourceKey") ? payload.getString("sourceKey") : requestContext.getDataCategory();					
				var columnRelations =	columnRelationsMap.get(sourceKey).parallelStream().collect(Collectors.toMap(ColumnRelation::getColumnName, Function.identity()));
				//var columnsRelations = columnRelationsMap.get(sourceKey).parallelStream().map(item -> item.getColumnName()).collect(Collectors.toList());
				
				JSONArray headers = payload.getJSONArray("headers");
					
				var rawColumns = IntStream.range(0, headers.length()).mapToObj(headerIndex -> headers.getString(headerIndex)).collect(Collectors.toMap(Function.identity(), Function.identity(), (x,y) -> y, LinkedHashMap::new));
				
			
				JSONArray dataJsonArr = payload.getJSONArray("data");
				
				var rows = IntStream.range(0, dataJsonArr.length()).parallel().mapToObj(dataIndex -> {
					JSONObject recordData = dataJsonArr.getJSONObject(dataIndex);
					var columnMap = rawColumns.values().parallelStream().filter(columnName -> columnRelations.containsKey(columnName))
							.collect(HashMap<UUID,String>::new, 
									(map,columnName) -> map.put(UUID.fromString(columnRelations.get(columnName).getUuid()), 
																(recordData.has(columnName) && !recordData.isNull(columnName)) ? ValidationUtil.isHavingValue(recordData.get(columnName).toString()) ? recordData.get(columnName).toString() : null : null), 
									HashMap<UUID,String>::putAll);		
							
					
					return new DataMap(columnMap);
				}).collect(Collectors.toList());				
				
				var map = new HashMap<String, List<DataMap>>();
				map.put(sourceKey, rows);				
				return map;
			}).flatMap(map -> map.entrySet().stream()).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
			log.info("JsonReaderService -> read() Completed processing JSONObject, uniqueId = {}", requestContext.getUniqueId());
			return rowsMap;
		} catch (Exception e) {
			log.error("JsonReaderService -> read() Failed to processing JSONObject, uniqueId = {}, message = {}", requestContext.getUniqueId(), e.getMessage());
			var readerException = new ReaderException("JSON Parsing failed, reason = " + e.getMessage(), e.getCause());
			readerException.setRequestContext(requestContext);
			throw readerException;
		}
	}
	
	
	protected JSONArray processJson(JSONObject payload) {
		JSONArray constructedPayload = new JSONArray();
		JSONObject textJson = null;
		JSONObject dataGridJson = null;
		JSONArray tabsArray = new JSONArray();
		for(String item : payload.keySet()){			
			switch (item.toUpperCase()) {
			case "TEXT":				
				textJson = (JSONObject) payload.get(item);				
				break;
				
			case "TABS":
				if(payload.get(item) instanceof JSONArray) {
					JSONArray tabs = (JSONArray) payload.get(item);					
					tabs.forEach(tab -> {
						JSONObject jTab = (JSONObject) tab;
						tabsArray.putAll(processJson(jTab));			
					});
				}	
				break;

			case "DATA_GRID":
				dataGridJson = (JSONObject)payload.get(item);
				break;

			default:				
				if(item.toUpperCase().matches("^[T]{1}[A]{1}[B]{1}[_]{1}\\d{1,}$")) {					
					JSONArray tab = (JSONArray) payload.get(item);
					if(!tab.isNull(0)) {
						JSONObject jSection = (JSONObject) tab.get(0);
						JSONArray temp = processJson(jSection);			
						if(!temp.isNull(0)) {
							JSONObject tempTab = (JSONObject) temp.get(0);	
							//log.info("JsonReaderService -> processJson() Completed processing JSONObject, tempTab-start = {}", tempTab);
							tempTab.put("sourceKey", requestContext.getDataCategory() + "{{" + item + "}}");	
							//log.info("JsonReaderService -> processJson() Completed processing JSONObject, tempTab-end = {}", tempTab);
							temp.put(0, tempTab);
						}
						constructedPayload.putAll(temp);
					}
					/*
					tab.forEach(section -> {
						JSONObject jSection = (JSONObject) section;
						constructedPayload.putAll(processJson(jSection));						
					});
					*/
				}
				break;
			}
		}
		
		if(textJson != null) {
			
			if(dataGridJson != null) {				
				JSONObject generated = constructJSONObject(textJson, dataGridJson);
				constructedPayload.put(generated);
			} 
			
			if(tabsArray != null && !tabsArray.isEmpty()) {
				for(Object obj : tabsArray) {					
					JSONObject generated = constructJSONObject(textJson, (JSONObject)obj);
					constructedPayload.put(generated);
				}
				
			} 
			
			if(dataGridJson == null && (tabsArray == null || tabsArray.isEmpty()))
				constructedPayload.put(textJson);
		} else {
			if(dataGridJson != null) {				
				constructedPayload.put(dataGridJson);
			} 
			if(tabsArray != null && !tabsArray.isEmpty()) {
				for(Object obj : tabsArray) {					
					constructedPayload.put((JSONObject)obj);
				}
				
			} 
		}
		return constructedPayload;
	}
	
	
	private JSONObject constructJSONObject(JSONObject textJson, JSONObject dataJson) {
		JSONArray textDataArray = (JSONArray)textJson.get("data");
		
		//Merging Headers
		JSONArray headers = (JSONArray) dataJson.get("headers");
		if(textJson.get("headers") != null && !((JSONArray)textJson.get("headers")).isEmpty())
			headers.putAll(textJson.get("headers"));
		
		JSONArray data = (JSONArray) dataJson.get("data"); new JSONArray();
		
		//Merging Data
		if(textDataArray != null && !textDataArray.isEmpty()) {
			JSONObject textData = (JSONObject)textDataArray.get(0);
			JSONArray dataGridData = (JSONArray) dataJson.get("data");
			data = new JSONArray();
			for(Object obj: dataGridData) {
				JSONObject value = (JSONObject) obj;
				data.put(mergeJSONObjects(value,textData));
			}
		}		
		JSONObject generated = new JSONObject();
		generated.put("headers", headers);
		generated.put("data", data);
		//if(dataJson.has("sourceKey"))
			//generated.put("sourceKey", dataJson.getString("sourceKey"));
		
		return generated;
	}
	
	private JSONObject mergeJSONObjects(JSONObject json1, JSONObject json2) {
        JSONObject mergedJSON = new JSONObject();
        try {
            // getNames(): Get an array of field names from a JSONObject.
            mergedJSON = new JSONObject(json1, JSONObject.getNames(json1));
            for (String crunchifyKey : JSONObject.getNames(json2)) {
                // get(): Get the value object associated with a key.
                mergedJSON.put(crunchifyKey, json2.get(crunchifyKey));
            }
        } catch (JSONException e) {
            // RunttimeException: Constructs a new runtime exception with the specified detail message.
            // The cause is not initialized, and may subsequently be initialized by a call to initCause.
            throw new RuntimeException("JSON Exception" + e);
        }
        return mergedJSON;
    }
	
}
