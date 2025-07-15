package com.adp.esi.digitech.file.processing.reader.text.service;

import java.util.ArrayList;
import java.util.HashMap;

import org.springframework.stereotype.Service;

import com.adp.esi.digitech.file.processing.ds.config.model.ColumnMetaData;
import com.adp.esi.digitech.file.processing.ds.config.model.LineMetaData;

@Service
public class TextPositionalExtractService extends AbstractTextExtractService {	
	
	public void extract(LineMetaData[] lineMetaDataArray) {
		for (LineMetaData lineMetaData : lineMetaDataArray) {			
			var columnIdentifierPositions = new HashMap<String, Integer[]>();
			var lineIdentifies = new ArrayList<String[]>();
			
			
			for(ColumnMetaData columnMetaData : lineMetaData.getColumnMetaData()) {				
				var column = columnMetaData.getColumn();
				var startPosition = columnMetaData.getStartPosition() - 1;
				var endPosition = columnMetaData.getEndPosition() - 1;
					
				if(column.startsWith("{{") && column.endsWith("}}")) {
					var uuidStr = uuidFun.apply(column);
					columnIdentifierPositions.put(uuidStr, new Integer[]{startPosition, endPosition});
				}
					
				if(columnMetaData.getLineIdentifier() == 'Y') {
					lineIdentifies.add(new String[] {column, startPosition + "", endPosition + ""});
				}
						
			}
						
			lineMetaData.setLineIdentifiers(lineIdentifies);
			lineMetaData.setColumnIdentifierPositions(columnIdentifierPositions);
		}
	}

}
