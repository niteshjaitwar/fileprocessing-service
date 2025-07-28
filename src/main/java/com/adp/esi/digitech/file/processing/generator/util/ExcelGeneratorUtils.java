package com.adp.esi.digitech.file.processing.generator.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.file.processing.ds.config.model.TargetDataFormat;
import com.adp.esi.digitech.file.processing.util.ValidationUtil;

@Service
public class ExcelGeneratorUtils {
	
	public final String DEFAULT_FORMAT = "xxxxx.xx";
	
	public final int  MAX_SENSITIVE_SHEET_NAME_LEN = 28;
	
	public HashMap<UUID,XSSFCellStyle> getCellStyleMap(Workbook workbook, Map<UUID, TargetDataFormat> targetFormatMap) {
		return (HashMap<UUID,XSSFCellStyle>) targetFormatMap.entrySet()
				.parallelStream().filter(entry -> Objects.nonNull(entry.getValue()) && ValidationUtil.isHavingValue(entry.getValue().getTargetType()))
				.collect(Collectors.toMap(Map.Entry::getKey, entry -> {
					var targetDataFormat = entry.getValue();
					var targetDecimalAllowed = targetDataFormat.getTargetDecimalAllowed();
					var targetFormat = targetDataFormat.getTargetFormat(); 
					return getCellStyle(workbook, ValidationUtil.isHavingValue(targetDecimalAllowed) ? targetDecimalAllowed : "0", ValidationUtil.isHavingValue(targetFormat) ? targetFormat : DEFAULT_FORMAT);
				}));
	}
	
	public XSSFCellStyle getCellStyle(Workbook workbook, String targetDecimalAllowed, String targetFormat) {
		var range = Integer.valueOf(targetDecimalAllowed);		
		var joined = IntStream.range(0, range).mapToObj(i -> "0").collect(Collectors.joining(""));
		
		var style=(XSSFCellStyle) workbook.createCellStyle();							
		var dataFormat = workbook.createDataFormat();
		
		targetFormat = targetFormat.trim().toLowerCase();
		
		var temp = "";
		
		switch (targetFormat) {
			case "xx xxx,xx":
			case "xx.xxx,xx":
			case "xx'xxx,xx":
			case "xxxxx,xx":
				temp = DEFAULT_FORMAT.split("\\.")[0];
				targetFormat = temp.substring(0, temp.length()-1) + "0";
				
				break;								
			case "xx,xxx.xx": 
			case DEFAULT_FORMAT:								
				temp = targetFormat.split("\\.")[0];
				targetFormat = temp.substring(0, temp.length()-1) + "0";
				
				break;								
							
			default:
				break;
		}
		
		if (Objects.nonNull(joined) && !joined.isBlank())
			targetFormat = targetFormat + "." + joined;
		
		targetFormat = targetFormat.replace('x', '#');	
		var num = dataFormat.getFormat(targetFormat);
		
		style.setDataFormat(num);		
		
		return style;
	}

	/*
	private Function<TargetDataFormat, XSSFCellStyle> getCellStyle(SXSSFWorkbook workbook) {		
		return targetDataFormat -> {			
			var targetType = targetDataFormat.getTargetType();
			var targetDecimalAllowed = targetDataFormat.getTargetDecimalAllowed();
			var targetFormat = targetDataFormat.getTargetFormat(); 
			
			if(ValidationUtil.isHavingValue(targetType)) {
				switch (targetType) {
					case "Number": 
						if(ValidationUtil.isHavingValue(targetDecimalAllowed)) {
							return getCellStyle(workbook, targetDecimalAllowed, ValidationUtil.isHavingValue(targetFormat) ? targetFormat : DEFAULT_FORMAT);
						}
					break;
				
					default:
					break;
				}
				
			}
			return null;
		};
		
		
		Function<TargetDataFormat, XSSFCellStyle> fun = targetDataFormat -> {
			
			var targetType = targetDataFormat.getTargetType();
			var targetDecimalAllowed = targetDataFormat.getTargetDecimalAllowed();
			var targetFormat = targetDataFormat.getTargetFormat(); 
			
			if(Objects.nonNull(targetType) && targetType.equals("Number") ) {
				if(Objects.nonNull(targetDecimalAllowed) && Objects.nonNull(targetFormat)) {
					return getCellStyle(workbook, targetDecimalAllowed, targetFormat);
				} else if(Objects.nonNull(targetDecimalAllowed)) {
					return getCellStyle(workbook, targetDecimalAllowed, "xxxxx.xx");
				} 
			}
			return null;
		};
		
	}
	//.collect(HashMap<UUID,XSSFCellStyle>::new, (m,entry)-> m.put(entry.getKey(), getCellStyle(workbook).apply(entry.getValue())), HashMap<UUID,XSSFCellStyle>::putAll);
	*/

}
