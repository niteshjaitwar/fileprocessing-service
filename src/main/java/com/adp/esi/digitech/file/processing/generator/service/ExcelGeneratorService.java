package com.adp.esi.digitech.file.processing.generator.service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.stream.IntStream;

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
import com.adp.esi.digitech.file.processing.model.Row;

import lombok.extern.slf4j.Slf4j;

@Service("excelGeneratorService")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class ExcelGeneratorService extends AbstractGeneratorService<Row, byte[]> {
	

	@Value("${pagination.excel.size}")
	int pageSize;
	
	@Autowired
	ExcelGeneratorUtils excelGeneratorUtils;
	
	@Override
	public byte[] generate(JSONObject outputFileRule, Map<String,DataSet<Row>> data) throws GenerationException {
		log.info("ExcelGeneratorService -> generate() Started Excel Generation, uniqueId = {}", requestContext.getUniqueId());
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try(XSSFWorkbook workbook = new XSSFWorkbook()) {
			var sheetsJsonArray = outputFileRule.getJSONArray("sheets");
		
			IntStream.range(0, sheetsJsonArray.length()).forEach(sheetIndex -> {
				var sheetJson = sheetsJsonArray.getJSONObject(sheetIndex);
				var sheetDatasetName = (String) sheetJson.get("dataSetName");
				var sheetData = data.get(sheetDatasetName);
				//log.info("ExcelGeneratorService -> generate() Excel Generation Sheet Dataset Name, uniqueId = {}, sheetDatasetName = {}", requestContext.getUniqueId(),sheetDatasetName);
				customGeneratorDynamicAutowireService.generate(workbook, sheetJson, sheetData, excelGeneratorUtils.getCellStyleMap(workbook, sheetData.getTargetFormatMap()), this.requestContext);
			});
			workbook.write(bos);
		} catch (IOException e) {
			log.info("ExcelGeneratorService -> generate() Failed Excel Generation, uniqueId = {}, error = {}", requestContext.getUniqueId(), e.getMessage());
			var generationException = new GenerationException("Failed at Excel Generation", e);
			generationException.setRequestContext(requestContext);
			throw generationException;
		}
		log.info("ExcelGeneratorService -> generate() Completed Excel Generation, uniqueId = {}", requestContext.getUniqueId());
		return bos.toByteArray();
	}
	
	
	
}
