package com.adp.esi.digitech.file.processing.generator.service;

import java.io.FileReader;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.file.processing.exception.GenerationException;
import com.adp.esi.digitech.file.processing.model.DataSet;
import com.adp.esi.digitech.file.processing.model.Row;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Service("largeCSVGeneratorService")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class LargeCSVGeneratorService extends AbstractLargeGeneratorService<Void, Void> {

	@Autowired
	private ObjectMapper objectMapper;

	@Override
	public Void generate(JSONObject outputFileRule, Map<String, DataSet<Void>> data) throws GenerationException {		
		log.info("LargeCSVGeneratorService -> generate() Started CSV Generation, uniqueId = {}", requestContext.getUniqueId());
		
		// Extracting values from outputFileRule
		var csvGeneratorUtils = customGeneratorUtilDynamicAutowireService.getCSVGeneratorUtils(outputFileRule, isTransformationRequired);
		var dataSetName = csvGeneratorUtils.getDatasetName();

		try (Writer fileWriter = Files.newBufferedWriter(getOutputPath(constructFileName(outputFileRule), "csv"),getCharset(csvGeneratorUtils.getEncoding()),
				StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
				CSVPrinter csvPrinter = new CSVPrinter(fileWriter, CSVFormat.Builder.create().setDelimiter(csvGeneratorUtils.getDelimeter()).setRecordSeparator(csvGeneratorUtils.getRowSeperator()).build())) {

			String requestDir = largeRequestFilePath + requestContext.getRequestUuid();
			Path transformDir = Paths.get(requestDir, "datasets", dataSetName, "transform");
			
			if (csvGeneratorUtils.isIncludeHeader()) {
				csvPrinter.printRecord(csvGeneratorUtils.generateHeaderStream());
				csvPrinter.flush();
			}

			try (Stream<Path> paths = Files.list(transformDir).filter(Files::isRegularFile)) {
				paths.forEach(path -> {
					try (var sReader = new FileReader(path.toFile())) {
						List<Row> rows = objectMapper.readValue(sReader, new TypeReference<List<Row>>() {});
						rows.forEach(row -> {
							try {
								csvPrinter.printRecord(csvGeneratorUtils.generateColumnStream(row));
							} catch (IOException e) {
								log.error("LargeCSVGeneratorService -> generate() Error writing row to CSV: {}", e.getMessage(), e);
								var generationException = new GenerationException(e.getMessage(), e.getCause());
								generationException.setRequestContext(requestContext);
								throw generationException;
							}
						});
						csvPrinter.flush();
					} catch (IOException e) {
						log.error("LargeCSVGeneratorService -> generate() Error processing file {}: {}", path.getFileName(), e.getMessage(), e);
						var generationException = new GenerationException(e.getMessage(), e.getCause());
						generationException.setRequestContext(requestContext);
						throw generationException;
					}
				});
			}
			log.info("LargeCSVGeneratorService -> generate() Completed CSV Generation, uniqueId = {}",	requestContext.getUniqueId());
			return null;

		} catch (IOException e) {
			log.error("LargeCSVGeneratorService -> generate() Failed CSV Generation, uniqueId = {}, message = {}", requestContext.getUniqueId(), e.getMessage(), e);
			var generationException =  new GenerationException("Failed to generate CSV: " + e.getMessage(), e);
			generationException.setRequestContext(requestContext);
			throw generationException;
		}
	}
}
