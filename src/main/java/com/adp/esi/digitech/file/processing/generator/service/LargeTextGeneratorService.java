package com.adp.esi.digitech.file.processing.generator.service;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.commons.collections4.map.HashedMap;
import org.json.JSONArray;
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
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;

import lombok.extern.slf4j.Slf4j;

@Service("largeTextGeneratorService")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class LargeTextGeneratorService extends AbstractLargeGeneratorService<Void, Void> {
	
	@Autowired
	Executor asyncExecutor;
	
	@Autowired
	TextGeneratorUtils textGeneratorUtils;
	
	@Override
	public Void generate(JSONObject outputFileRule, Map<String,DataSet<Void>> data) throws GenerationException {
		
		try {
			var requestDir = largeRequestFilePath + requestContext.getRequestUuid();
			var dataSetDir = requestDir + "/datasets";			
			
			log.info("LargeTextGeneratorService -> generate() Started Text Generation, uniqueId = {}", requestContext.getUniqueId());
			StringBuilder sb = new StringBuilder();
			
			var fileName =  constructFileName(outputFileRule);
			var outputPath = getOutputPath(fileName, "txt");
			var charSet = getCharset(outputFileRule.optString("encoding"));
			var processType = outputFileRule.optString("processing","default");
			var classType = "position".equalsIgnoreCase(processType) ? PositionLineGeneratorService.class : LineGeneratorService.class;
			var writer = Files.newBufferedWriter(outputPath, charSet, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
			
			var isRowRepeat = textGeneratorUtils.isRowRepeat(outputFileRule);		
	
			sb = textGeneratorUtils.constructHeaders(classType, outputFileRule, sb, this.requestContext);
			
			writer.write(sb.toString());
			writer.flush();
			
			//cleaning StringBuilder
			sb.delete(0, sb.length());
			
			if (outputFileRule.has("data") && !outputFileRule.isNull("data") && outputFileRule.getJSONArray("data").length() > 0) {
				var rowJsonArray = outputFileRule.getJSONArray("data");
				if (isRowRepeat) {
					IntStream.range(0, rowJsonArray.length()).forEach(index -> {
						var rowJson = rowJsonArray.getJSONObject(index);
						var dataSetId = rowJson.getString("dataSetName");
						var currentDataSetTransformDir = Paths.get(dataSetDir + "/" + dataSetId + "/transform");
						
						try(Stream<Path> paths = Files.list(currentDataSetTransformDir).filter(path -> path.toFile().isFile())){
							paths.forEach(path -> {
								try {
									var sReader = new FileReader(path.toFile());
									var rows =objectMapper.readValue(sReader, new TypeReference<List<Row>>() {});
									var generatedData = rows.stream()
										.map(row -> customGeneratorDynamicAutowireService.generate(classType, rowJson, row, this.requestContext))
										.filter(ValidationUtil::isHavingValue).collect(Collectors.joining("\n"));
									if(ValidationUtil.isHavingValue(generatedData)) {
										writer.write(generatedData + "\n");
										writer.flush();
									}
								} catch(IOException e) {
									throw new GenerationException(e.getMessage(), e);
								}
							});
						} catch(IOException e) {
							throw new GenerationException(e.getMessage(), e);
						}
					});
				} else {
					Map<String, Map<String, List<String>>> dataSetIndexedFileReferenceData = IntStream.range(0, rowJsonArray.length())
							.mapToObj(index -> {
						var rowJson = rowJsonArray.getJSONObject(index);
						var dataSetId = rowJson.getString("dataSetName");
						var currentDataSetTransformDir = Paths.get(dataSetDir + "/" + dataSetId + "/transform");						
						var currentDataSetTransformIndexDir = Paths.get(currentDataSetTransformDir + "/indexed");
						var referceUuid = rowJson.getString("referenceId");							
						
						
							
						try(Stream<Path> paths = Files.list(currentDataSetTransformDir).filter(path -> path.toFile().isFile())){
							if(Files.notExists(currentDataSetTransformIndexDir)) {
								Files.createDirectories(currentDataSetTransformIndexDir);
							}
							var fileReferenceData =  paths.map(path -> {
								try {
									var sReader = new FileReader(path.toFile());
									var rows =objectMapper.readValue(sReader, new TypeReference<List<Row>>() {});
									var groupRows = rows.parallelStream()
											.collect(Collectors.groupingBy(row -> row.getColumns().get(UUID.fromString(referceUuid)).getTargetValue()));
									var keys = groupRows.keySet().stream().collect(Collectors.toList());
									var currentFileName = path.toFile().getName();
									
									CompletableFuture.runAsync(() -> {						
										write(currentDataSetTransformIndexDir, currentFileName, groupRows);
									}, asyncExecutor);
									var metadata = new HashedMap<String, List<String>>();
									metadata.put(currentFileName, keys);
									return metadata;
								} catch(IOException e) {
									throw new GenerationException(e.getMessage(), e);
								}
							}).flatMap(map -> map.entrySet().stream()).collect(Collectors.toMap(Entry::getKey, Entry::getValue, (oldData, newData) -> newData, HashMap::new));
							
							var dataSetFileReferenceData = new HashedMap<String, Map<String, List<String>>>();
							dataSetFileReferenceData.put(dataSetId, fileReferenceData);
							return dataSetFileReferenceData;
						} catch(IOException e) {
							throw new GenerationException(e.getMessage(), e);
						}
					}).flatMap(map -> map.entrySet().stream()).collect(Collectors.toMap(Entry::getKey, Entry::getValue, (oldData, newData) -> newData, HashMap::new));	
					
					log.info("LargeTextGeneratorService -> generate() Completed creating indexing for transformed data, uniqueId = {}", requestContext.getUniqueId());
					
					//var allDataSetIndexedChunks = getAllDataSetChunks(rowJsonArray);
					//log.info("LargeTextGeneratorService -> generate() Completed reading all indexed chunks, uniqueId = {}", requestContext.getUniqueId());
					var firstRowJson = rowJsonArray.getJSONObject(0);
					var firstRowDataId = firstRowJson.getString("dataSetName");
					var firstRowReferceUuid = firstRowJson.getString("referenceId");
					var firstRowDataSetTransformDir = Paths.get(dataSetDir + "/" + firstRowDataId + "/transform");
					
					try(Stream<Path> firstDataSetPaths = Files.list(firstRowDataSetTransformDir).filter(path -> path.toFile().isFile())){
						firstDataSetPaths.forEach(path -> {
							try {
								var sReader = new FileReader(path.toFile());
								var rows =objectMapper.readValue(sReader, new TypeReference<List<Row>>() {});
								
								rows.stream().forEach(firstRow -> {
									try {
										StringBuilder temp = new StringBuilder();
										var content = customGeneratorDynamicAutowireService.generate(classType, firstRowJson, firstRow, this.requestContext);
										if(ValidationUtil.isHavingValue(content)) {
											temp.append(content);
											temp.append("\n");
										}
										
										var referenceGeneratedData = IntStream.range(0, rowJsonArray.length()).mapToObj(index -> {
											if(index > 0) {
												var firstRowReferceIdData = firstRow.getColumns().get(UUID.fromString(firstRowReferceUuid)).getTargetValue();
												if(ValidationUtil.isHavingValue(firstRowReferceIdData)) {
													var referenceRowJson = rowJsonArray.getJSONObject(index);
													var referenceDataSetId = referenceRowJson.getString("dataSetName");
													var referceUuid = referenceRowJson.getString("referenceId");
													var referceDataSetTransformDir = Paths.get(dataSetDir + "/" + referenceDataSetId + "/transform");
													var referceDataSetTransformIndexDir = Paths.get(referceDataSetTransformDir + "/indexed");
													
													var optional = dataSetIndexedFileReferenceData.get(referenceDataSetId).entrySet().parallelStream()
													.filter(entry -> entry.getValue().contains(firstRowReferceIdData))
													.findFirst();
													
													if(optional.isPresent()) {
														var entry = optional.get();
														var referceIndexedFileName = entry.getKey();
														
														//var referceDataSetIndexedChunks = allDataSetIndexedChunks.get(referenceDataSetId);
														//var referceIndexedFileChunks = referceDataSetIndexedChunks.get(referceIndexedFileName);
														//var referenceRows = readSourceRows(referceIndexedFileChunks,firstRowReferceIdData); 
														var referenceRows = readSourceRows(referceDataSetTransformIndexDir.toString(), referceIndexedFileName, firstRowReferceIdData);
														
														if(Objects.nonNull(referenceRows) && !referenceRows.isEmpty()) {
															var referenceRow = referenceRows.get(0);
															var referenceRowReferceIdData = referenceRow.getColumns().get(UUID.fromString(referceUuid)).getTargetValue();
															if(ValidationUtil.isHavingValue(referenceRowReferceIdData) 
																	&& firstRowReferceIdData.equals(referenceRowReferceIdData))
																return customGeneratorDynamicAutowireService.generate(classType, referenceRowJson, referenceRow, this.requestContext);
														}
													}											
												}
											
											}	
											
											return null;
										}).filter(ValidationUtil::isHavingValue).collect(Collectors.joining("\n"));
										if(ValidationUtil.isHavingValue(referenceGeneratedData)) {
											temp.append(referenceGeneratedData + "\n");
										}
										if(ValidationUtil.isHavingValue(temp.toString())) {
											writer.write(temp.toString());
											writer.flush();
										}
										
									} catch(IOException e) {
										throw new GenerationException(e.getMessage(), e);
									}
								});
								
								
							} catch(IOException e) {
								throw new GenerationException(e.getMessage(), e);
							}
						});
					}
					
					
				}
			}		
			writer.flush();
			writer.close();
			log.info("LargeTextGeneratorService -> generate() Completed Text Generation, uniqueId = {}", requestContext.getUniqueId());
			return null;
		} catch (IOException e) {
			log.info("LargeTextGeneratorService -> generate() Failed Text Generation, uniqueId = {}, message = {}", requestContext.getUniqueId(), e.getMessage());
			throw new GenerationException(e.getMessage(), e);
		}
	}
	
	private <T> void write(Path dir, String fileName,  Map<String,List<T>> data) throws GenerationException {	
		
        try {
        	var file = Paths.get(dir + "/" + fileName ).toFile();
        	objectMapper.writeValue(file, data);
		} catch (IOException e) {
			throw new GenerationException(e.getMessage(), e);
		}
    }
	
	
	@SuppressWarnings("unused")
	private Map<String, Map<String, byte[]>> getAllDataSetChunks(JSONArray rowJsonArray) {
		var requestDir = largeRequestFilePath + requestContext.getRequestUuid();
		var dataSetDir = requestDir + "/datasets";	
		
		var filteredDataSet = IntStream.range(0, rowJsonArray.length()).mapToObj(index -> {
			var rowJson = rowJsonArray.getJSONObject(index);
			var dataSetId = rowJson.getString("dataSetName");
			return dataSetId;
		}).collect(Collectors.toSet());
		
		
		return filteredDataSet.stream().map(dataSetId -> {			
			var currentDataSetTransformDir = Paths.get(dataSetDir + "/" + dataSetId + "/transform");						
			var currentDataSetTransformIndexDir = Paths.get(currentDataSetTransformDir + "/indexed");
			
			var item = new HashMap<String, Map<String, byte[]>>();
			item.put(dataSetId, readAllChunks(currentDataSetTransformIndexDir));
			return item;
		}).flatMap(m -> m.entrySet().stream())
		.collect(Collectors.toMap(Entry::getKey, Entry::getValue));
	}
	private Map<String, byte[]> readAllChunks(Path currentDataSetTransformIndexDir) {
		
		try(Stream<Path> paths = Files.walk(currentDataSetTransformIndexDir).filter(path -> path.toFile().isFile())) {
			return paths.collect(Collectors.toMap(path -> path.toFile().getName(), path -> {
				
				try(var sReader = new FileReader(path.toFile())) { 
					return objectMapper.writeValueAsBytes(objectMapper.readTree(sReader));
					
				} catch (IOException e) {
					log.info("LargeTextGeneratorService -> readAllChunks() FileReader, Failed Text Generation, uniqueId = {}, message = {}", requestContext.getUniqueId(), e.getMessage());
					throw new GenerationException(e.getMessage(), e);
				}
				
				/*
				try(var fis = Files.newInputStream(path)) {
					
					return IOUtils.toByteArray(fis);
					
					var bos = new ByteArrayOutputStream();
					byte[] buffer = new byte[1024*1024];
					int bytesRead = 0;
					while((bytesRead = fis.read(buffer)) > 0) {
						bos.write(buffer, 0, bytesRead);
					}
					return bos.toByteArray();
					
					
				} catch (IOException e) {
					throw new GenerationException(e.getMessage(), e);
				}
				*/
			}));
			//HashMap<String, byte[]>::new, (map, path) -> map.put(path.toFile().getName(), Files.readAllBytes(path)), HashMap<String, byte[]>::putAll
		} catch (IOException e) {
			log.info("LargeTextGeneratorService -> readAllChunks() Failed Text Generation, uniqueId = {}, message = {}", requestContext.getUniqueId(), e.getMessage());
			throw new GenerationException(e.getMessage(), e);
		}
		
	}
	
	private List<Row> readSourceRows(String tempDir, String fileName, String key) {
		var sFile = new File(tempDir + "/" + fileName ); //+ ".json"
		try(var sReader = new FileReader(sFile);				
			JsonParser jsonParser = jsonFactory.createParser(sReader)) {			
			var rootNode = objectMapper.readTree(jsonParser);
			var dataNode = rootNode.path(key);
			return objectMapper.convertValue(dataNode, new TypeReference<List<Row>>() {});
		} catch (IOException e) {
			log.error("LargeTextGeneratorService -> readSourceRows() Text Generation Failed, uniqueId = {}, File = {}, key = {}, message = {}", requestContext.getUniqueId(), fileName, key, e.getMessage());
			throw new GenerationException(e.getMessage(), e);
		}
		
		//var sFile = Path.of(tempDir+ "/" + fileName).toFile();
		//InputStream is = new FileInputStream(sFile);
		//var map = objectMapper.readValue(jsonParser, new TypeReference<Map<String, List<Row>>> () {});
		//return map.get(key);
	}
	
	@SuppressWarnings("unused")
	private List<Row> readSourceRows(byte[] data, String key) {		
		try { 
			var rootNode = objectMapper.readTree(data);
			var dataNode = rootNode.path(key);
			return objectMapper.convertValue(dataNode, new TypeReference<List<Row>>() {});
		} catch (IOException e) {
			throw new GenerationException(e.getMessage(), e);
		}
	}

}
