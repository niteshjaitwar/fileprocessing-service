package com.adp.esi.digitech.file.processing.reader.text.service;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

import org.springframework.stereotype.Service;

import com.adp.esi.digitech.file.processing.ds.config.model.LineMetaData;
import com.adp.esi.digitech.file.processing.ds.model.ColumnRelation;
import com.adp.esi.digitech.file.processing.model.DataMap;
import com.adp.esi.digitech.file.processing.util.ValidationUtil;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class LineReaderService {
	
	private static final String CODE_VALUE_PAIR = "position + 1";
	private static final String CODE_AGGREGATES = "position // 2";
	
	private Function<String, String> uuidFun = (input) -> input.substring(2, input.length() - 2);
	
	private String applyTrim(String data) {
		return Optional.ofNullable(data).map(String::strip).filter(s -> !s.isEmpty()).orElse(null);
	}
	
	private String getData(String[] data, int position) {
		return (Objects.nonNull(data) && data.length > position) ? data[position] : null;
	}
	
	private String getData(String line, Integer[] positions) {
		int startPosition = positions[0];
		int endPosition = positions[1];		
		return ValidationUtil.isHavingValue(line) ? line.substring(startPosition, endPosition) : null;
	}
	
	private Map<UUID, String> readColumns(String[] data, Map<String, Integer[]> columnsPosition, Map<String, ColumnRelation> columnRelationMap) {
		return columnRelationMap.keySet().stream().collect(HashMap<UUID, String>::new,
				(map, key) -> map.put(UUID.fromString(key), columnsPosition.containsKey(key) ? applyTrim(getData(data, columnsPosition.get(key)[0])) : null), HashMap::putAll);
	}
	
	private Map<UUID, String> readColumns(String line, Map<String, Integer[]> columnsPosition, Map<String, ColumnRelation> columnRelationMap) {
		return columnRelationMap.keySet().stream().collect(HashMap<UUID, String>::new,
				(map, key) -> map.put(UUID.fromString(key), columnsPosition.containsKey(key) ? applyTrim(getData(line, columnsPosition.get(key))) : null), HashMap::putAll);
	}

	
	private Map<UUID, String> readCodes(String[] data, Map<String, String[]> codeIdentifers) {
		
		var dataMap = new HashMap<UUID, String>();
		
		int length = data.length;
	    if (length % 2 != 0) {
	    	log.info("data array cannot be split into two equal parts.");
	        return null;
	    }	
		
		var firstCode = (String[])codeIdentifers.values().toArray()[0];
		var codeValue = (String)firstCode[1];
		switch(codeValue) {
			case CODE_VALUE_PAIR:
				for (int i = 0; i < data.length; i++) {
					if(codeIdentifers.containsKey(data[i])) {
						var codeIdentifer = codeIdentifers.get(data[i]);
						var column = codeIdentifer[0];
						
						if(column.startsWith("{{") && column.endsWith("}}")) {
							var uuidStr = uuidFun.apply(column);
							var uuid = UUID.fromString(uuidStr);
							dataMap.put(uuid, data[i + 1]); // Assuming the next position has the value
						}
					}
					i++;
				}
			    break;
			case CODE_AGGREGATES:
		        int mid = length / 2;
		        String[] firstHalf = Arrays.copyOfRange(data, 0, mid);
		        String[] secondHalf = Arrays.copyOfRange(data, mid, length);
		        
		        for (int i = 0; i < firstHalf.length; i++) {
		        	if(codeIdentifers.containsKey(firstHalf[i])) {
						var codeIdentifer = codeIdentifers.get(firstHalf[i]);
						var column = codeIdentifer[0];
						
						if(column.startsWith("{{") && column.endsWith("}}")) {
							var uuidStr = uuidFun.apply(column);
							var uuid = UUID.fromString(uuidStr);
							dataMap.put(uuid, secondHalf[i]);
						}
					}
		        }
		        break;
		}
		
		
		return dataMap;
	}
	
	
	public DataMap read(String[] data, Map<String, Integer[]> columnsPosition, Map<String, ColumnRelation> columnRelationMap) {
		var columnDataMap = readColumns(data, columnsPosition, columnRelationMap);
		return new DataMap(columnDataMap);

	}
	
	public DataMap read(String[] data, LineMetaData lineMetaData, Map<String, ColumnRelation> columnRelationMap) {
		var columnDataMap = readColumns(data, lineMetaData.getColumnIdentifierPositions(), columnRelationMap);
		
		if(Objects.nonNull(lineMetaData.getCodeIdentifers()) && !lineMetaData.getCodeIdentifers().isEmpty() 
				&&  lineMetaData.getCodeStartIndex() > -1 ) {
			
			var codesArr = Arrays.copyOfRange(data, lineMetaData.getCodeStartIndex(), data.length);
			
			var codeMap = readCodes(codesArr, lineMetaData.getCodeIdentifers());
			columnDataMap.putAll(codeMap);
		}
		return new DataMap(columnDataMap);

	}
	
	/**
	 * This method is used to get the data map from the line by position
	 * 
	 * @param line
	 * @param lineMetaData
	 * @param columnRelationMap
	 * @return
	 */
	public DataMap read(String line, LineMetaData lineMetaData, Map<String, ColumnRelation> columnRelationMap) {		
		var columnDataMap = readColumns(line, lineMetaData.getColumnIdentifierPositions(), columnRelationMap);
		return new DataMap(columnDataMap);
	}
}
