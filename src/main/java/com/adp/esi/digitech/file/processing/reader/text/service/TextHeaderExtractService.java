package com.adp.esi.digitech.file.processing.reader.text.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.stereotype.Service;

import com.adp.esi.digitech.file.processing.ds.config.model.LineMetaData;
import com.adp.esi.digitech.file.processing.ds.model.ColumnRelation;

@Service
public class TextHeaderExtractService extends AbstractTextExtractService {
	
	public void extract(LineMetaData lineMetaData, String[] headers, Map<String, ColumnRelation> namedColumnRealtionsMap) {		
		
		var lineIdentifies = new ArrayList<String[]>();
		var columnIdentifierPositions = new HashMap<String, Integer[]>();
		
		var lineIdentifiersKeyMap = Stream.of(lineMetaData.getColumnMetaData())
				  					.filter(columnMetaData -> columnMetaData.getLineIdentifier() == 'Y')
				  					.map(columnMetaData -> columnMetaData.getColumn())
				  					.collect(Collectors.toMap(column -> column, column -> column));
		
		for (int i = 0; i < headers.length; i++) {
			if(lineIdentifiersKeyMap.containsKey(headers[i])) {
				lineIdentifies.add(new String[] {lineIdentifiersKeyMap.get(headers[i]), i + ""});
			}	
			
			if(namedColumnRealtionsMap.containsKey(headers[i])) {
				var selectedColumnRelation = namedColumnRealtionsMap.get(headers[i]);
				columnIdentifierPositions.put(selectedColumnRelation.getUuid(), new Integer[]{i});
			}
		}
		
		lineMetaData.setLineIdentifiers(lineIdentifies);
		lineMetaData.setColumnIdentifierPositions(columnIdentifierPositions);
	}

}
