package com.adp.esi.digitech.file.processing.generator.service;

import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.file.processing.exception.GenerationException;
import com.adp.esi.digitech.file.processing.generator.util.TextGeneratorUtils;
import com.adp.esi.digitech.file.processing.model.DataSet;
import com.adp.esi.digitech.file.processing.model.Row;
import com.adp.esi.digitech.file.processing.util.ValidationUtil;

import lombok.extern.slf4j.Slf4j;

@Service("textGeneratorService")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class TextGeneratorService extends AbstractGeneratorService<Row, byte[]> {
	
	@Autowired
	TextGeneratorUtils textGeneratorUtils;

	@Override
	public byte[] generate(JSONObject outputFileRule, Map<String, DataSet<Row>> dataMap) throws GenerationException {
		log.info("TextGeneratorService -> generate() Started Text Generation, uniqueId = {}", requestContext.getUniqueId());
		StringBuilder sb = new StringBuilder();
		
		var isRowRepeat = textGeneratorUtils.isRowRepeat(outputFileRule);		
		var charset = getCharset(outputFileRule.optString("encoding"));
		var processType = outputFileRule.optString("processing","default");
		var classType = "position".equalsIgnoreCase(processType) ? PositionLineGeneratorService.class : LineGeneratorService.class;
		sb = textGeneratorUtils.constructHeaders(classType, outputFileRule, sb, this.requestContext);
		

		if (outputFileRule.has("data") && !outputFileRule.isNull("data") && outputFileRule.getJSONArray("data").length() > 0) {
			var rowJsonArray = outputFileRule.getJSONArray("data");
			if (isRowRepeat) {
				var dataLines = IntStream.range(0, rowJsonArray.length()).mapToObj(index -> {
					var rowJson = rowJsonArray.getJSONObject(index);
					var dataSetName = rowJson.getString("dataSetName");
					var rows = dataMap.get(dataSetName).getData();
					return rows.stream()
							.map(row -> customGeneratorDynamicAutowireService.generate(classType, rowJson, row, this.requestContext))
							.filter(ValidationUtil::isHavingValue).collect(Collectors.joining("\n"));
				}).collect(Collectors.joining("\n"));
				if(ValidationUtil.isHavingValue(dataLines)) {
					sb.append(dataLines);
					sb.append("\n");
				}
			} else {
				var firstRowJson = rowJsonArray.getJSONObject(0);
				var firstRowDataSetName = firstRowJson.getString("dataSetName");
				
				var firstRowDataRows = new ArrayList<>(dataMap.get(firstRowDataSetName).getData());
				var firstRowReferceUuid = firstRowJson.getString("referenceId");
						
				while(!firstRowDataRows.isEmpty()) {
					Row currentRow = firstRowDataRows.get(0);
					var dataLines = IntStream.range(0, rowJsonArray.length()).mapToObj(index -> {
						var rowJson = rowJsonArray.getJSONObject(index);						
						if(index == 0) {							
							return customGeneratorDynamicAutowireService.generate(classType, rowJson, currentRow, this.requestContext);							
						} else {							
							var dataSetName = rowJson.getString("dataSetName");
							var referceUuid = rowJson.getString("referenceId");
							var rows = dataMap.get(dataSetName).getData();
							var optionalRow = rows.parallelStream().filter(item -> {								
								var lhs = String.valueOf(currentRow.getColumns().get(UUID.fromString(firstRowReferceUuid)).getValue());
								var rhs = String.valueOf(item.getColumns().get(UUID.fromString(referceUuid)).getValue());
								return lhs.equals(rhs);
							}).findFirst();
							if(optionalRow.isPresent()) {
								var row = optionalRow.get();
								return customGeneratorDynamicAutowireService.generate(classType, rowJson, row, this.requestContext);
							}
							return null;			
						}
					}).filter(ValidationUtil::isHavingValue).collect(Collectors.joining("\n"));
					if(ValidationUtil.isHavingValue(dataLines)) {
						sb.append(dataLines);
						sb.append("\n");
					}
					firstRowDataRows.remove(0);
				}
			}
		}
		log.info("TextGeneratorService -> generate() Completed Text Generation, uniqueId = {}", requestContext.getUniqueId());
		return sb.toString().getBytes(charset);
	}
}
