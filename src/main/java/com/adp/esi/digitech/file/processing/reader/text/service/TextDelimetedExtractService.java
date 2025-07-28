package com.adp.esi.digitech.file.processing.reader.text.service;

import java.util.ArrayList;
import java.util.HashMap;

import org.springframework.stereotype.Service;

import com.adp.esi.digitech.file.processing.ds.config.model.ColumnMetaData;
import com.adp.esi.digitech.file.processing.ds.config.model.LineMetaData;
import com.adp.esi.digitech.file.processing.util.ValidationUtil;

@Service
public class TextDelimetedExtractService extends AbstractTextExtractService {
	
	public void extract(LineMetaData[] lineMetaDataArray) {		
		for (LineMetaData lineMetaData : lineMetaDataArray) {			
			var columnIdentifierPositions = new HashMap<String, Integer[]>();
			var lineIdentifies = new ArrayList<String[]>();
			var codeIdentifers = new HashMap<String, String[]>();
			int index = -1;
			int codeStartIndex = -1;
			var iscodeStartIndexSet = false;
			
			for(ColumnMetaData columnMetaData : lineMetaData.getColumnMetaData()) {
				/*
				if (ValidationUtil.isHavingValue(columnMetaData.getPrefix()))
					index++;
				*/
				if (ValidationUtil.isHavingValue(columnMetaData.getCode()) 
						&& ValidationUtil.isHavingValue(columnMetaData.getCodeValue())) {				
					index++;
					codeStartIndex = index;
					var codeDataArray = new String[]{columnMetaData.getColumn(), columnMetaData.getCodeValue()};
					if(!iscodeStartIndexSet) {
						lineMetaData.setCodeStartIndex(codeStartIndex);
						iscodeStartIndexSet = true;
					}
					codeIdentifers.put(columnMetaData.getCode(), codeDataArray);
					
				} else {
					index++;
					var column = columnMetaData.getColumn();
					
					if(column.startsWith("{{") && column.endsWith("}}")) {
						var uuidStr = uuidFun.apply(column);
						columnIdentifierPositions.put(uuidStr, new Integer[]{index});
					}
					
					if(columnMetaData.getLineIdentifier() == 'Y') {
						lineIdentifies.add(new String[] {column, index + ""});
					}
				}

				/*
				if (ValidationUtil.isHavingValue(columnMetaData.getSuffix()))
					index++;
				*/				
			}
						
			lineMetaData.setLineIdentifiers(lineIdentifies);
			lineMetaData.setColumnIdentifierPositions(columnIdentifierPositions);			
			lineMetaData.setCodeIdentifers(codeIdentifers);
		}
	}

}
