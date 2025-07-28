package com.adp.esi.digitech.file.processing.generator.service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.json.JSONObject;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.file.processing.exception.GenerationException;
import com.adp.esi.digitech.file.processing.model.DataSet;
import com.adp.esi.digitech.file.processing.model.Row;

import lombok.extern.slf4j.Slf4j;

@Service("csvGeneratorService")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class CSVGeneratorService extends AbstractGeneratorService<Row, byte[]> {
	

	@Override
	public byte[] generate(JSONObject outputFileRule, Map<String, DataSet<Row>> dataMap) throws GenerationException {
		log.info("CSVGeneratorService -> generate() Started CSV Generation, uniqueId = {}",	requestContext.getUniqueId());
		
		// Extracting values from outputFileRule
		var csvGeneratorUtils = customGeneratorUtilDynamicAutowireService.getCSVGeneratorUtils(outputFileRule, isTransformationRequired);
		
		var dataSetName = csvGeneratorUtils.getDatasetName();
		var dataSet = dataMap.get(dataSetName);

		if (Objects.isNull(dataSet)) {
			log.warn("CSVGeneratorService -> generate()  No data found for DataSet {}", dataSetName);
			return new byte[0];
		}

		try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
			OutputStreamWriter outputStreamWriter = new OutputStreamWriter(byteArrayOutputStream, getCharset(csvGeneratorUtils.getEncoding()));
			CSVPrinter csvPrinter = new CSVPrinter(outputStreamWriter, CSVFormat.Builder.create().setDelimiter(csvGeneratorUtils.getDelimeter()).setRecordSeparator(csvGeneratorUtils.getRowSeperator()).build())) {

			if (csvGeneratorUtils.isIncludeHeader())
				csvPrinter.printRecord(csvGeneratorUtils.generateHeaderStream());

			for (Row currentRow : dataSet.getData()) {
				csvPrinter.printRecord(csvGeneratorUtils.generateColumnStream(currentRow));
			}
			csvPrinter.flush();
			return byteArrayOutputStream.toByteArray();
		} catch (IOException e) {
			log.error("CSVGeneratorService -> generate() Failed CSV Generation, uniqueId = {}, message = {}", requestContext.getUniqueId(), e.getMessage());
			var generationException = new GenerationException(e.getMessage(), e.getCause());
			generationException.setRequestContext(requestContext);
			throw generationException;
		}
	}
}
