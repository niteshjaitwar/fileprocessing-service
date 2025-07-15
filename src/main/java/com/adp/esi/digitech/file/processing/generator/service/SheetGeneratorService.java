package com.adp.esi.digitech.file.processing.generator.service;

import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.poi.ss.usermodel.DataConsolidateFunction;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.AreaReference;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFPivotTable;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.file.processing.ds.config.model.TargetDataFormat;
import com.adp.esi.digitech.file.processing.exception.GenerationException;
import com.adp.esi.digitech.file.processing.generator.util.ExcelGeneratorUtils;
import com.adp.esi.digitech.file.processing.model.Column;
import com.adp.esi.digitech.file.processing.model.RequestContext;
import com.adp.esi.digitech.file.processing.model.Row;
import com.adp.esi.digitech.file.processing.util.FileUtils;
import com.adp.esi.digitech.file.processing.util.ValidationUtil;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Service("sheetGeneratorService")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class SheetGeneratorService  {
	
	@Value("${large.request.file.path}")
	private String largeRequestFilePath;
	
	@Autowired
	ExcelGeneratorUtils excelGeneratorUtils;
	
	@Autowired
	PivotGeneratorService pivotGeneratorService;
	
	private RequestContext requestContext;
	
	private Map<UUID, TargetDataFormat> targetFormatMap;
	
	private Map<UUID,XSSFCellStyle> targetCellStyleMap;
	
	private String isTransformationRequired;	
	
	@Autowired(required = true)
	public SheetGeneratorService(RequestContext requestContext, Map<UUID, TargetDataFormat> targetFormatMap,
			Map<UUID, XSSFCellStyle> targetCellStyleMap) {
		this.requestContext = requestContext;
		this.targetFormatMap = targetFormatMap;
		this.targetCellStyleMap = targetCellStyleMap;
	}

	@Autowired
	FileUtils fileUtils;
	
	@Autowired
	ObjectMapper objectMapper;
	
	public SXSSFSheet generate(SXSSFWorkbook workbook,JSONObject sheetRule, String dataSetId) throws IOException {
		log.info("SheetGeneratorService -> generate() Started Sheet Generation, uniqueId = {}", requestContext.getUniqueId());
		var requestDir = largeRequestFilePath + requestContext.getRequestUuid();
		var dataSetDir = requestDir + "/datasets";
		var dir = Paths.get(dataSetDir + "/" + dataSetId + "/transform");
		
		Row[] tempRow = {null};
		var sheetName = constructSheetName(workbook, sheetRule, null);
		log.info("SheetGeneratorService -> generate() Completed Sheet name Generation, uniqueId = {}, sheetName = {}", requestContext.getUniqueId(), sheetName);	
		
		SXSSFSheet sheet = ValidationUtil.isHavingValue(sheetName)? workbook.createSheet(sheetName) : workbook.createSheet();
		JSONArray headers = sheetRule.getJSONArray("headers");
		
		constructHeader(sheet, headers, 0);
		int[] position = {1};
		if(sheetRule.has("additional_headers") && !sheetRule.isNull("additional_headers")) {
			JSONArray additional_headers_array = sheetRule.getJSONArray("additional_headers");
			
			for (Object additional_headers_obj : additional_headers_array) {
				var additional_headers = (JSONArray)additional_headers_obj;
				org.apache.poi.ss.usermodel.Row additionalRowHeader = sheet.createRow(position[0]);
				
				IntStream.range(0, additional_headers.length()).forEach(index -> {
					var value = !additional_headers.isNull(index) ? additional_headers.getString(index): "";
					additionalRowHeader.createCell(index).setCellValue(value);
				});
				
				position[0] = ++position[0];
			}
			
		
		}
		try(Stream<Path> paths = Files.list(dir).filter(path -> path.toFile().isFile())){
			paths.forEach(path -> {
				try {
					var data = getData(path);
					
					if(Objects.nonNull(data) && !data.isEmpty()) {
						IntStream.range(0, data.size()).forEach((rowIndex) ->{
							if(Objects.isNull(tempRow[0])) {
								tempRow[0] = data.get(rowIndex);
							}
							
							constructDataRow(workbook, sheet, position[0], data.get(rowIndex), headers);
							position[0] = ++position[0];
						});
					}
					
				} catch (IOException e) {
					log.info("SheetGeneratorService -> generate() Failed Sheet Generation, uniqueId = {}, error = {}", requestContext.getUniqueId(), e.getMessage());
					var generationException = new GenerationException("Failed at Sheet Generation", e);
					generationException.setRequestContext(requestContext);
					throw generationException;
				}
			});
		}
		log.info("SheetGeneratorService -> generate() Completed Sheet Generation, uniqueId = {}", requestContext.getUniqueId());
		workbook.setSheetName(workbook.getSheetIndex(sheet), constructSheetName(workbook, sheetRule, tempRow[0]));
		sheet.flushRows();
		return sheet;
	}
	
	public XSSFSheet generate(XSSFWorkbook workbook,JSONObject sheetRule, List<Row> data) {
		log.info("SheetGeneratorService -> generate() Started Sheet Generation, uniqueId = {}", requestContext.getUniqueId());
		
		Row firstRowData = Objects.nonNull(data) && !data.isEmpty() ? data.get(0) : null;
		
		var sheetName = constructSheetName(workbook, sheetRule, firstRowData);
		
		log.info("SheetGeneratorService -> generate() Completed Sheet name Generation, uniqueId = {}, sheetName = {}", requestContext.getUniqueId(), sheetName);
		
		if(sheetRule.has("pivot") && !sheetRule.isNull("pivot")) {
			var pivotJson = sheetRule.getJSONObject("pivot");
			var sourceSheetRequired = pivotJson.optString("sourceSheetRequired", "N");
			if("N".equalsIgnoreCase(sourceSheetRequired)) {
				constructPivot(workbook, data, sheetRule, sheetName);
				return null;
			}
			constructPivot(workbook, data, sheetRule, "Pivot_" + sheetName);	
		}
		
		XSSFSheet sheet = ValidationUtil.isHavingValue(sheetName)? workbook.createSheet(sheetName) : workbook.createSheet();
		
		JSONArray headers = sheetRule.getJSONArray("headers");
		
		constructHeader(sheet, headers, 0);
		
		
		int[] position = {1};
		if(sheetRule.has("additional_headers") && !sheetRule.isNull("additional_headers")) {
			JSONArray additional_headers_array = sheetRule.getJSONArray("additional_headers");
			
			for (Object additional_headers_obj : additional_headers_array) {
				var additional_headers = (JSONArray)additional_headers_obj;
				org.apache.poi.ss.usermodel.Row additionalRowHeader = sheet.createRow(position[0]);
				
				IntStream.range(0, additional_headers.length()).forEach(index -> {
					var value = !additional_headers.isNull(index) ? additional_headers.getString(index): "";
					additionalRowHeader.createCell(index).setCellValue(value);
				});
				
				position[0] = ++position[0];
			}
		}

		if(Objects.nonNull(data) && !data.isEmpty()) {
			IntStream.range(0, data.size()).forEach((rowIndex) ->{
				constructDataRow(workbook, sheet, (rowIndex + position[0]), data.get(rowIndex), headers);
			});
		}
		log.info("SheetGeneratorService -> generate() Completed Sheet Generation, uniqueId = {}, sheetName = {}", requestContext.getUniqueId(), sheetName);
		
		
		
		return sheet;
	}
	
	private void constructDataRow(Workbook workbook, Sheet sheet, int position, Row dataRow, JSONArray headers) {		
		org.apache.poi.ss.usermodel.Row row = sheet.createRow(position);
		var columnsMap = dataRow.getColumns();

		IntStream.range(0, headers.length()).forEach(index -> {	
			var headerJsonObj = headers.getJSONObject(index);
			var columnUuid = headerJsonObj.has("field") && !headerJsonObj.isNull("field") ? headerJsonObj.getString("field") : "";
			var key = UUID.fromString(columnUuid);
			Column column  = columnsMap.get(key);
			
			if(column == null) {
				log.error("SheetGeneratorService -> generate() Started Sheet Generation, uniqueId = {}, key = {}, columnsMap = {}", requestContext.getUniqueId(), key, columnsMap);					
			}
			var dataStr = "";
			if(column != null) {
				var columnData = isTransformationRequired.equalsIgnoreCase("N") ? column.getSourceValue() : column.getTargetValue();
				dataStr = (columnData != null && columnData instanceof String) ? String.valueOf(columnData) : "";
				
			}
			var cell =  row.createCell(index);
			cell.setCellValue(dataStr);
			
			if(targetFormatMap.containsKey(key) && ValidationUtil.isHavingValue(dataStr)) {						
				var targetFormatObj = targetFormatMap.get(key);
				if(Objects.nonNull(targetFormatObj)) {
					var columnData = isTransformationRequired.equalsIgnoreCase("N") ? column.getValue() : column.getTransformedValue();
					dataStr = (columnData != null && columnData instanceof String) ? String.valueOf(columnData) : "";
					
					var targetType = targetFormatObj.getTargetType();
					var targetDecimalAllowed = targetFormatObj.getTargetDecimalAllowed();
					var targetFormat = targetFormatObj.getTargetFormat(); 
					
					if(ValidationUtil.isHavingValue(targetType) && targetType.equals("Number") && NumberUtils.isParsable(dataStr)) {
						
						if(!ValidationUtil.isHavingValue(targetDecimalAllowed) && !ValidationUtil.isHavingValue(targetFormat)) {
							cell.setCellValue(Double.valueOf(dataStr));
						} else {
							XSSFCellStyle style = null;
							
							if(ValidationUtil.isHavingValue(targetDecimalAllowed)) {
								style = targetCellStyleMap.containsKey(key) && Objects.nonNull(targetCellStyleMap.get(key)) 
										? targetCellStyleMap.get(key) : excelGeneratorUtils.getCellStyle(workbook, targetDecimalAllowed, ValidationUtil.isHavingValue(targetFormat) ? targetFormat : excelGeneratorUtils.DEFAULT_FORMAT);
							} else if(Objects.nonNull(targetFormat)) {
								if(dataStr.contains(".")) {
									BigDecimal de = new BigDecimal(dataStr);										
									de = de.setScale(3, RoundingMode.HALF_UP);
									dataStr = String.valueOf(de.doubleValue());
								}
								dataStr = dataStr.contains(".") ? dataStr.replaceAll("0*$", "").replaceAll("\\.$", "") : dataStr;
								var tempTargetDecimalAllowed = dataStr.contains(".") ? dataStr.split("\\.")[1].length() : 0;
								var temp = String.valueOf(tempTargetDecimalAllowed);									
								style = targetCellStyleMap.containsKey(key) && Objects.nonNull(targetCellStyleMap.get(key)) ? targetCellStyleMap.get(key) : excelGeneratorUtils.getCellStyle(workbook, temp, targetFormat);										
							}
							cell.setCellStyle(style);
							cell.setCellValue(Double.valueOf(dataStr));															
						}
						
					}
				}
			}
			
		});
		
	}
	
	
	
	private void constructHeader(Sheet sheet, JSONArray headers, int postion) {
		org.apache.poi.ss.usermodel.Row rowHeader = sheet.createRow(postion);
		IntStream.range(0, headers.length()).forEach(index -> {
			var headerJsonObj = headers.getJSONObject(index);
			var columnName = headerJsonObj.has("name") && !headerJsonObj.isNull("name") ? headerJsonObj.getString("name") : "";
			rowHeader.createCell(index).setCellValue(columnName);
		});
	}
	
	private String constructSheetName(Workbook workbook, JSONObject sheetRule, Row firstRowData) {
		var sheetName = (sheetRule.has("sheetName") && !sheetRule.isNull("sheetName")) ? fileUtils.constructFileName(null, sheetRule.getJSONObject("sheetName"), firstRowData):"";
		
		if(sheetName.length() < excelGeneratorUtils.MAX_SENSITIVE_SHEET_NAME_LEN) {
			return sheetName;
		}		
		String trimmedSheetname = sheetName.substring(0, excelGeneratorUtils.MAX_SENSITIVE_SHEET_NAME_LEN);
	    trimmedSheetname = trimmedSheetname + "_" + (workbook.getNumberOfSheets() + 1); 
	    // we still need to warn about the trimming as the original sheet name won't be available
	    log.warn("Sheet '{}' will be added with a trimmed name '{}' for MS Excel compliance.", sheetName, trimmedSheetname);
	    return trimmedSheetname;
	    
	}
	
	private List<Row> getData(Path path) throws IOException {
		//var sFile = new File(location.toFile());
		var sReader = new FileReader(path.toFile());
		return objectMapper.readValue(sReader, new TypeReference<List<Row>>() {});
	}
	
	public void setRequestContext(RequestContext requestContext) {
		this.requestContext = requestContext;
	}
	
	public void setIsTransformationRequired(String isTransformationRequired) {
		this.isTransformationRequired = isTransformationRequired;
	}
	
	private XSSFCellStyle getCellStyle(Workbook workbook, UUID key, String targetFormat, String targetDecimalAllowed) {	
		
		return targetCellStyleMap.containsKey(key) && Objects.nonNull(targetCellStyleMap.get(key)) 
				? targetCellStyleMap.get(key) : excelGeneratorUtils.getCellStyle(workbook, targetDecimalAllowed, targetFormat);
	}
	
	public void constructPivot(XSSFWorkbook workbook, List<Row> data , JSONObject sheetRule, String sheetName) {
		try {
			log.info("SheetGeneratorService -> constructPivot() Started Pivot Sheet Generation, uniqueId = {}", requestContext.getUniqueId());
			XSSFSheet sheet = ValidationUtil.isHavingValue(sheetName)? workbook.createSheet(sheetName) : workbook.createSheet("Pivot_sheet" + workbook.getNumberOfSheets());
			var output = pivotGeneratorService.generate(sheetRule, data);
			
			if (output == null || output.length == 0) {
				log.info("SheetGeneratorService -> constructPivot() No Data Found for Pivot Sheet Generation, uniqueId = {}", requestContext.getUniqueId());
				return;
			}
			var pivotRule = sheetRule.getJSONObject("pivot");
			var noOfRows = pivotRule.getJSONArray("rows").length();
			var valuesJSONArray = pivotRule.getJSONArray("values");
			var valueJson = valuesJSONArray.getJSONObject(0);
			var column = valueJson.getString("value");
			var key = UUID.fromString(column);
			var isUseDefaultStyle = false;
			var isFormatSet = false;
			
			XSSFCellStyle style = null;
			
			if(targetFormatMap.containsKey(key)) {
				isFormatSet = true;
			} 
			
			String targetDecimalAllowed = null;
			String targetFormat = null;
			
			if(isFormatSet) {
				var targetFormatObj = targetFormatMap.get(key);
				targetDecimalAllowed = targetFormatObj.getTargetDecimalAllowed();
				targetFormat = targetFormatObj.getTargetFormat();			
				
				if(!ValidationUtil.isHavingValue(targetDecimalAllowed) && !ValidationUtil.isHavingValue(targetFormat)) {
					isUseDefaultStyle = false;
				}
				
				targetFormat = ValidationUtil.isHavingValue(targetFormat) ? targetFormat : excelGeneratorUtils.DEFAULT_FORMAT;
				
				if(ValidationUtil.isHavingValue(targetDecimalAllowed)) {					
					style = getCellStyle(workbook, key, targetFormat, targetDecimalAllowed);
					isUseDefaultStyle = true;
				}
				
			}
			
			for (int i = 0; i < output.length; i++) {
				var row = sheet.createRow(i);
				for (int j = 0; j < output[i].length; j++) {
					var cell = row.createCell(j);
					var value = output[i][j];
					if(j < noOfRows) {
						cell.setCellValue(value);
						continue;
					}
					if(!isFormatSet) {
						cell.setCellValue(value);
						continue;
					}
					
					if (!NumberUtils.isParsable(value)) {
						if(!isUseDefaultStyle) {
							style = getCellStyle(workbook, key, "0", targetFormat);
						}
						cell.setCellStyle(style);
						cell.setCellValue(value);
						continue;
					}
					
					if(!isUseDefaultStyle) {								
						if(value.contains(".")) {
							BigDecimal de = new BigDecimal(value);										
							de = de.setScale(3, RoundingMode.HALF_UP);
							value = String.valueOf(de.doubleValue());
						}
						value = value.contains(".") ? value.replaceAll("0*$", "").replaceAll("\\.$", "") : value;
						var tempTargetDecimalAllowed = value.contains(".") ? value.split("\\.")[1].length() : 0;
						style = getCellStyle(workbook, key, String.valueOf(tempTargetDecimalAllowed), targetFormat);
					}							
					cell.setCellStyle(style);
					cell.setCellValue(Double.valueOf(value));
					
					
				}
			}
			log.info("SheetGeneratorService -> constructPivot() Completed Pivot Sheet Generation, uniqueId = {}", requestContext.getUniqueId());
		} catch (Exception e) {
			log.error("ExcelGeneratorService -> constructPivot() Failed at Pivot Generation, uniqueId = {}, error = {}", requestContext.getUniqueId(), e.getMessage());
			var generationException = new GenerationException("Failed at Pivot Generation", e);
			generationException.setRequestContext(requestContext);
			throw generationException;
		}
	}
	
	public void constructPivot(XSSFWorkbook workbook, XSSFSheet sourceSheet, JSONObject sheetRule) {
		try {
		log.info("SheetGeneratorService -> constructPivot() Started Pivot Sheet Generation, uniqueId = {}", requestContext.getUniqueId());
		var pivotJson = sheetRule.getJSONObject("pivot");
		if((!pivotJson.has("rows") || pivotJson.isNull("rows")) || (!pivotJson.has("values") || pivotJson.isNull("values"))) {
			return;
		}
		
		var sheetHeaders = sheetRule.getJSONArray("headers");
		
		var indexMap = new HashMap<String, Integer>();
		IntStream.range(0, sheetHeaders.length()).forEach(index -> {
			var header = sheetHeaders.getJSONObject(index);
			var field = header.getString("field");
			indexMap.put(field, index);
		});
		
		var sourceSheetName = sourceSheet.getSheetName();
		
		JSONArray headers = sheetRule.getJSONArray("headers");
		var lastcolumnIndex = headers.length() - 1;
		var lastRowIndex = (sourceSheet.getLastRowNum() + 1);
		
		var areaReference = new AreaReference("A1:" + getColumnLabel(lastcolumnIndex) + lastRowIndex, workbook.getSpreadsheetVersion());
		var cellReference = new CellReference("A1");
		
		
		XSSFSheet pivotSheet = workbook.createSheet(sourceSheetName + "_PivotSheet");		
		XSSFPivotTable pivotTable = pivotSheet.createPivotTable(areaReference, cellReference, sourceSheet);
		
		
		var rowLabelArray = pivotJson.getJSONArray("rows");		
		rowLabelArray.forEach(rowLabel -> {
			var rowLabelUUID = (String) rowLabel;
			if(indexMap.containsKey(rowLabelUUID)) {
				var index = indexMap.get(rowLabelUUID);
				pivotTable.addRowLabel(index);
			}
		});
		
		if(pivotJson.has("columns") && !pivotJson.isNull("columns")) {
			var columnLabelArray = pivotJson.getJSONArray("columns");			
			columnLabelArray.forEach(columnLabel -> {
				var columnLabelUUID = (String) columnLabel;
				if(indexMap.containsKey(columnLabelUUID)) {
					var index = indexMap.get(columnLabelUUID);
					pivotTable.addColLabel(index);
				}
			});
		}
		
		if(pivotJson.has("filters") && !pivotJson.isNull("filters")) {
			var filterLabelArray = pivotJson.getJSONArray("filters");			
			filterLabelArray.forEach(filterLabel -> {
				var filterLabelUUID = (String) filterLabel;
				if(indexMap.containsKey(filterLabelUUID)) {
					var index = indexMap.get(filterLabelUUID);
					pivotTable.addReportFilter(index);
				}
			});
		}
		
		var valueLabelArray = pivotJson.getJSONArray("values");
		valueLabelArray.forEach(valueLabel -> {
			var valueLabelJSON = (JSONObject) valueLabel;
			var valueLabelUUID = valueLabelJSON.getString("value");
			var operation = valueLabelJSON.getString("operation");
			if (indexMap.containsKey(valueLabelUUID)) {
				var index = indexMap.get(valueLabelUUID);
				switch (operation.toUpperCase()){
					case "SUM":
						pivotTable.addColumnLabel(DataConsolidateFunction.SUM, index);
						break;
					case "AVERAGE":
						pivotTable.addColumnLabel(DataConsolidateFunction.AVERAGE, index);
						break;
					case "MAX":
						pivotTable.addColumnLabel(DataConsolidateFunction.MAX, index);
						break;
					case "MIN":
						pivotTable.addColumnLabel(DataConsolidateFunction.MIN, index);
						break;
					case "COUNT":
						pivotTable.addColumnLabel(DataConsolidateFunction.COUNT, index);
						break;
					default:
						throw new IllegalArgumentException("Unexpected value: " + operation.toUpperCase());
				}
				
			}
		});
		
		var sourceSheetRequired = pivotJson.has("sourceSheetRequired") && !pivotJson.isNull("sourceSheetRequired") ? pivotJson.getString("sourceSheetRequired") : "N";		
		if (sourceSheetRequired.equalsIgnoreCase("N")) {
			workbook.setSheetHidden(workbook.getSheetIndex(sourceSheet), true);
			//workbook.setSheetName(workbook.getSheetIndex(pivotSheet), sourceSheetName);
		}
		
		log.info("SheetGeneratorService -> constructPivot() Completed Pivot Sheet Generation, uniqueId = {}", requestContext.getUniqueId());
		} catch (Exception e) {
			log.error("ExcelGeneratorService -> constructPivot() Failed at Pivot Generation, uniqueId = {}, error = {}", requestContext.getUniqueId(), e.getMessage());
			var generationException = new GenerationException("Failed at Pivot Generation", e);
			generationException.setRequestContext(requestContext);
			throw generationException;
		}
	}
	
	private String getColumnLabel(int index ) {
		var columnName = new StringBuilder();
		
		while(index >= 0) {
			columnName.insert(0, (char) ('A' + (index % 26)));
			index = (index/26) - 1;
		}
		return columnName.toString();
	}
}
