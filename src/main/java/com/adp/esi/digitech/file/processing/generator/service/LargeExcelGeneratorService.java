package com.adp.esi.digitech.file.processing.generator.service;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.file.processing.exception.GenerationException;
import com.adp.esi.digitech.file.processing.generator.util.ExcelGeneratorUtils;
import com.adp.esi.digitech.file.processing.model.DataSet;

import lombok.extern.slf4j.Slf4j;

@Service("largeExcelGeneratorService")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class LargeExcelGeneratorService extends AbstractLargeGeneratorService<Void, Void>{
	
	
	@Value("${pagination.excel.size}")
	int pageSize;	
	
	@Autowired
	ExcelGeneratorUtils excelGeneratorUtils;
	
	@Override
	public Void generate(JSONObject outputFileRule, Map<String,DataSet<Void>> data) throws GenerationException {
		try {
			SXSSFWorkbook workbook = new SXSSFWorkbook(pageSize);
			log.info("LargeExcelGeneratorService -> generate() Started Excel Generation, uniqueId = {}", requestContext.getUniqueId());
			workbook.setCompressTempFiles(true);
			
			var fileName =  constructFileName(outputFileRule);			
			//var channel = FileChannel.open(outputPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);		
			var sheetsJsonArray = outputFileRule.getJSONArray("sheets");
			var pivotSheetNameMap = new HashMap<String, JSONObject>();
			IntStream.range(0, sheetsJsonArray.length()).forEach(sheetIndex -> {
				var sheetJson = sheetsJsonArray.getJSONObject(sheetIndex);
				var sheetDatasetName = (String) sheetJson.get("dataSetName");
				var sheetData = data.get(sheetDatasetName);
				try {
					var sheet = customGeneratorDynamicAutowireService.generateLarge(workbook, sheetJson, sheetData, excelGeneratorUtils.getCellStyleMap(workbook, sheetData.getTargetFormatMap()), this.requestContext);
					
					if(sheetJson.has("pivot") && !sheetJson.isNull("pivot")) {
						var sheetName = sheet.getSheetName();
						pivotSheetNameMap.put(sheetName, sheetJson);
					}
				} catch (IOException e) {
					throw new GenerationException(e.getMessage(), e);
				}
			});
		
			//channel.close();
			var outputPath = getOutputPath(fileName, "xlsx");
			if(pivotSheetNameMap.isEmpty()) {				
				FileOutputStream fileOut = new FileOutputStream(outputPath.toFile());
				workbook.write(fileOut);
				fileOut.flush();
				fileOut.close();
				workbook.close();
				return null;
			}
			
				
			var tempOutputPath = getOutputPath("temp_"+fileName, "xlsx");
			var tempFile = tempOutputPath.toFile();
			FileOutputStream tempFileOut = new FileOutputStream(tempFile);
			workbook.write(tempFileOut);
			tempFileOut.flush();
			tempFileOut.close();
			workbook.dispose();
			
			//Creating in-memory workbook as pivot required whole sheet data
			OPCPackage pkg = OPCPackage.open(tempFile);
			XSSFWorkbook pivotWorkbook = new XSSFWorkbook(pkg);
			
			pivotSheetNameMap.entrySet().stream().forEach(entry -> {
				var sheetName = entry.getKey();
				var sheetJson = entry.getValue();
				var sourceSheet = pivotWorkbook.getSheet(sheetName);
				customGeneratorDynamicAutowireService.generatePivot(pivotWorkbook, sourceSheet, sheetJson, this.requestContext);
			});
			
			FileOutputStream pivotFileOut = new FileOutputStream(outputPath.toFile());
			pivotWorkbook.write(pivotFileOut);
			pivotFileOut.flush();
			pivotFileOut.close();				
			pivotWorkbook.close();		
			tempFile.delete();
			
			log.info("LargeExcelGeneratorService -> generate() Completed Excel Generation, uniqueId = {}", requestContext.getUniqueId());
		} catch (IOException | InvalidFormatException e) {
			throw new GenerationException(e.getMessage(), e);
		}
		return null;
	}

}

