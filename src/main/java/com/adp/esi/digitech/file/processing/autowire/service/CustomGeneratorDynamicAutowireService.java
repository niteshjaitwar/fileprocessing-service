package com.adp.esi.digitech.file.processing.autowire.service;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.context.WebApplicationContext;

import com.adp.esi.digitech.file.processing.exception.GenerationException;
import com.adp.esi.digitech.file.processing.generator.service.AbstractLineGeneratorService;
import com.adp.esi.digitech.file.processing.generator.service.IGeneratorService;
import com.adp.esi.digitech.file.processing.generator.service.SheetGeneratorService;
import com.adp.esi.digitech.file.processing.model.DataSet;
import com.adp.esi.digitech.file.processing.model.RequestContext;
import com.adp.esi.digitech.file.processing.model.Row;

@Service("customGeneratorDynamicAutowireService")
public class CustomGeneratorDynamicAutowireService {

	private final WebApplicationContext webApplicationContext;
	
	@Autowired
	public CustomGeneratorDynamicAutowireService(WebApplicationContext webApplicationContext) {
		this.webApplicationContext = webApplicationContext;
	}
	
	public <T extends IGeneratorService<Row, byte[]>> byte[] generate(Class<T> type, JSONObject outputFileRule, Map<String, DataSet<Row>> dataSetMap, RequestContext requestContext) throws GenerationException {
		IGeneratorService<Row, byte[]> generatorService = webApplicationContext.getBean(type);
		var isTransformationRequired = transformationRequired(outputFileRule);
		generatorService.setRequestContext(requestContext);
		generatorService.setIsTransformationRequired(isTransformationRequired);
		return generatorService.generate(outputFileRule, dataSetMap);
	}
	
	public <T extends IGeneratorService<Void, Void>> void generateLarge(Class<T> type, JSONObject outputFileRule, Map<String, DataSet<Void>> dataSetMap, RequestContext requestContext) throws GenerationException {
		IGeneratorService<Void, Void> generatorService = webApplicationContext.getBean(type);
		var isTransformationRequired = transformationRequired(outputFileRule);
		generatorService.setRequestContext(requestContext);
		generatorService.setIsTransformationRequired(isTransformationRequired);
		generatorService.generate(outputFileRule, dataSetMap);
	}
	
	
	public XSSFSheet generate(XSSFWorkbook workbook,JSONObject sheetRule, DataSet<Row> dataSet,HashMap<UUID, XSSFCellStyle> targetCellStyleMap, RequestContext requestContext) {
		SheetGeneratorService sheetGeneratorService = webApplicationContext.getBean(SheetGeneratorService.class, requestContext, dataSet.getTargetFormatMap(), targetCellStyleMap);
		//sheetGeneratorService.setRequestContext(requestContext);
		var isTransformationRequired = transformationRequired(sheetRule);		
		sheetGeneratorService.setIsTransformationRequired(isTransformationRequired);
		return sheetGeneratorService.generate(workbook, sheetRule, dataSet.getData());
	}
	
	public SXSSFSheet generateLarge(SXSSFWorkbook workbook,JSONObject sheetRule, DataSet<Void> dataSet,HashMap<UUID, XSSFCellStyle> targetCellStyleMap, RequestContext requestContext) throws IOException {
		SheetGeneratorService sheetGeneratorService = webApplicationContext.getBean(SheetGeneratorService.class, requestContext, dataSet.getTargetFormatMap(), targetCellStyleMap);
		//sheetGeneratorService.setRequestContext(requestContext);
		var isTransformationRequired = transformationRequired(sheetRule);		
		sheetGeneratorService.setIsTransformationRequired(isTransformationRequired);
		return sheetGeneratorService.generate(workbook, sheetRule, dataSet.getId());
	}
	
	public void generatePivot(XSSFWorkbook workbook, XSSFSheet sourceSheet, JSONObject sheetRule, RequestContext requestContext) {
		SheetGeneratorService sheetGeneratorService = webApplicationContext.getBean(SheetGeneratorService.class, requestContext, null, null);
		//sheetGeneratorService.setRequestContext(requestContext);
		var isTransformationRequired = transformationRequired(sheetRule);		
		sheetGeneratorService.setIsTransformationRequired(isTransformationRequired);
		sheetGeneratorService.constructPivot(workbook, sourceSheet, sheetRule);
	}
	
	public <T extends AbstractLineGeneratorService> String generate(Class<T> type, JSONObject colRule, Row data, RequestContext requestContext) {
		AbstractLineGeneratorService lineGeneratorService = webApplicationContext.getBean(type);		
		var isTransformationRequired = transformationRequired(colRule);
		lineGeneratorService.setRequestContext(requestContext);
		lineGeneratorService.setIsTransformationRequired(isTransformationRequired);
		return lineGeneratorService.generate(colRule, data);
	}
	
	private String transformationRequired(JSONObject rule)  {
		return Objects.nonNull(rule) ? rule.optString("isTransformationRequired", "Y") : "Y";
	}
}
