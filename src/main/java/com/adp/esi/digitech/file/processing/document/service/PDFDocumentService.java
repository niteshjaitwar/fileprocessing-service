package com.adp.esi.digitech.file.processing.document.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.file.processing.autowire.service.CustomGeneratorDynamicAutowireService;
import com.adp.esi.digitech.file.processing.ds.service.DatastudioConfigurationService;
import com.adp.esi.digitech.file.processing.exception.ConfigurationException;
import com.adp.esi.digitech.file.processing.exception.GenerationException;
import com.adp.esi.digitech.file.processing.generator.service.PdfGeneratorService;
import com.adp.esi.digitech.file.processing.model.Column;
import com.adp.esi.digitech.file.processing.model.DataSet;
import com.adp.esi.digitech.file.processing.model.RequestContext;
import com.adp.esi.digitech.file.processing.model.Row;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class PDFDocumentService {

	@Autowired
	CustomGeneratorDynamicAutowireService customGeneratorDynamicAutowireService;

	@Autowired
	DatastudioConfigurationService dataStudioConfigurationService;

	public byte[] preview(String bu, String platform, String dataCategory, String documentId) {
		var configuration = dataStudioConfigurationService.findConfigurationDataBy(bu, platform, dataCategory);
		var outputFileRules = configuration.getOutputFileRules();
		log.debug("outputFileRules: {}", outputFileRules);
		return previewWithPayload(bu, platform, dataCategory, documentId, outputFileRules);
	}

	public byte[] previewWithPayload(String bu, String platform, String dataCategory, String documentId,
			String outputFileRules) {

		var requestContext = new RequestContext();
		requestContext.setBu(bu);
		requestContext.setPlatform(platform);
		requestContext.setDataCategory(dataCategory);
		requestContext.setUniqueId(documentId);
		try {

			var outputFileRulesJSON = new JSONArray(outputFileRules);

			var optionalIndex = IntStream.range(0, outputFileRulesJSON.length()).filter(index -> {
				var outputFileRule = outputFileRulesJSON.getJSONObject(index);

				if (!outputFileRule.has("outputFileType") || outputFileRule.isNull("outputFileType"))
					return false;

				if (!outputFileRule.has("documentId") || outputFileRule.isNull("documentId"))
					return false;

				var outputFileType = outputFileRule.getString("outputFileType");
				var currentDocumentId = outputFileRule.getString("documentId");

				return outputFileType.equalsIgnoreCase("pdf") && currentDocumentId.equals(documentId);

			}).findFirst();

			if (!optionalIndex.isPresent()) {
				throw new ConfigurationException("No matching configuration found for given request");
			}
			int index = optionalIndex.getAsInt();
			var outputFileRule = outputFileRulesJSON.getJSONObject(index);

			return customGeneratorDynamicAutowireService.generate(PdfGeneratorService.class, outputFileRule,
					initiateDataSets(outputFileRule), requestContext);

		} catch (GenerationException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			var configurationException =  new ConfigurationException("PDF preview failed - " + e.getMessage(), e);
			configurationException.setRequestContext(requestContext);
			throw e;
		}

	}

	private Map<String, DataSet<Row>> initiateDataSets(JSONObject outputFileType) {
		var pageJsonArray = outputFileType.getJSONArray("pages");

		return IntStream.range(0, pageJsonArray.length()).mapToObj(index -> {
			var pageJosn = pageJsonArray.getJSONObject(index);
			var dataSet = new DataSet<Row>();
			dataSet.setId(pageJosn.getString("dataSetName"));
			var mappingsArray = pageJosn.getJSONArray("mappings");
			var columnMap = IntStream.range(index, mappingsArray.length()).mapToObj(mIndex -> {
				var mapping = mappingsArray.getJSONObject(mIndex);
				var name = mapping.getString("name");
				var uuid = mapping.getString("field");
				var value = mapping.getString("defaultValue");
				var column = new Column(name, value, UUID.fromString(uuid), null);
				column.setTargetValue(value);
				return column;
			}).collect(Collectors.toMap(Column::getUuid, Function.identity(), (o, n) -> o));
			var rows = List.of(new Row(columnMap));
			dataSet.setData(rows);
			return dataSet;
		}).collect(Collectors.toMap(DataSet::getId, Function.identity(), (o, n) -> o));
	}
}
