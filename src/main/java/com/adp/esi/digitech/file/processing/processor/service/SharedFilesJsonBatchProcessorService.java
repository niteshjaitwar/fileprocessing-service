package com.adp.esi.digitech.file.processing.processor.service;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.adp.esi.digitech.file.processing.ds.config.model.DataSetRules;
import com.adp.esi.digitech.file.processing.ds.config.model.FileMetaData;
import com.adp.esi.digitech.file.processing.ds.config.model.InputRule;
import com.adp.esi.digitech.file.processing.ds.config.model.Reference;
import com.adp.esi.digitech.file.processing.ds.config.model.TargetDataFormat;
import com.adp.esi.digitech.file.processing.ds.model.ColumnRelation;
import com.adp.esi.digitech.file.processing.enums.ProcessType;
import com.adp.esi.digitech.file.processing.enums.RequestStatus;
import com.adp.esi.digitech.file.processing.enums.TargetLocation;
import com.adp.esi.digitech.file.processing.enums.ValidationType;
import com.adp.esi.digitech.file.processing.exception.ConfigurationException;
import com.adp.esi.digitech.file.processing.exception.GenerationException;
import com.adp.esi.digitech.file.processing.exception.MetadataValidationException;
import com.adp.esi.digitech.file.processing.exception.ProcessException;
import com.adp.esi.digitech.file.processing.exception.ReaderException;
import com.adp.esi.digitech.file.processing.exception.TransformationException;
import com.adp.esi.digitech.file.processing.exception.ValidationException;
import com.adp.esi.digitech.file.processing.model.ChunkDataMap;
import com.adp.esi.digitech.file.processing.model.DataMap;
import com.adp.esi.digitech.file.processing.model.DataSet;
import com.adp.esi.digitech.file.processing.model.RequestPayload;
import com.adp.esi.digitech.file.processing.util.FileUtils;
import com.adp.esi.digitech.file.processing.util.FileValidationUtils;
import com.adp.esi.digitech.file.processing.util.ValidationUtil;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;

import lombok.extern.slf4j.Slf4j;

@Service
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
@Deprecated
public class SharedFilesJsonBatchProcessorService extends AbstractAsyncProcessorService<Void> {

	@Autowired
	FileUtils fileUtils;
	
	@Autowired
	FileValidationUtils fileValidationUtils;

	@Value("${large.request.file.path}")
	String largeRequestFilePath;

	public void constructDefaults() {
		super.constructDefaults(requestContext.getBu(), requestContext.getPlatform(), requestContext.getDataCategory());
	}

	@Override
	public Void process(RequestPayload request) throws IOException, ReaderException, ConfigurationException,
			ValidationException, TransformationException, GenerationException, ProcessException {
		log.info(
				"SharedFilesJsonBatchProcessorService -> process() Received JSON request for processing, uniqueId = {}, request = {}",
				request.getUniqueId(), request);

		this.updateRequest(request, RequestStatus.Started);

		this.iFilesService = objectUtilsService.objectProviderSharedFilesService.getObject(requestContext,
				TargetLocation.SharedDrive);

		var requestDir = largeRequestFilePath + requestContext.getRequestUuid();
		var sourceDir = requestDir + "/source";
		var dataSetDir = requestDir + "/datasets";
		this.constructDefaults();

		String inputRulesJson = configurationData.getInputRules();
		String outputRulesJson = configurationData.getOutputFileRules();
		String filesInfoJson = configurationData.getFilesInfo();
		String datasetRulesJson = configurationData.getDataRules();
		
		

		List<InputRule> inputRules = this.getInputRules(inputRulesJson);
		Map<String, FileMetaData> fileMetaDataMap = ValidationUtil.isHavingValue(filesInfoJson)
				? this.getFileMetaDataMap(filesInfoJson)
				: this.getFileMetaDataMap(inputRules);

		List<DataSetRules> dataSetRules = this.getDataSetRules(datasetRulesJson);

		var dataSetRulesMap = Objects.nonNull(dataSetRules)
				? dataSetRules.stream().collect(Collectors.toMap(DataSetRules::getDataSetId, Function.identity()))
				: new HashMap<String, DataSetRules>();
		
		
		var clauseMap = getClause(dataSetRulesMap);		
		
		var documents = load(request, ProcessType.chunks);
		
		//Perform Metadata Validation
		var isMetaValidationsFound = documents.parallelStream().map(document -> {	
			
			var sourceKey = document.getSourceKey();
			var fileMetaData = fileMetaDataMap.get(sourceKey);
			
			if(fileMetaData == null) {
				var optFileMetaData = fileMetaDataMap.keySet().parallelStream().filter(key -> key.startsWith(sourceKey)).findFirst();				
				fileMetaData = optFileMetaData.isPresent() ? fileMetaDataMap.get(optFileMetaData.get()): null;
			}
			
			var fileExtension = fileMetaData != null ? fileMetaData.getType() : fileUtils.getFileExtension(document.getLocalPath());
			return fileValidationUtils.validate(document.getLocalPath(), fileExtension.toLowerCase());
			
		}).reduce(false, Boolean::logicalOr);		
			
		if(isMetaValidationsFound) {
			var metadataValidationException = new MetadataValidationException("Failed to process basic validation like file type and size");
			metadataValidationException.setRequestContext(requestContext);
			throw metadataValidationException;
		}
		
		log.info("SharedFilesJsonBatchProcessorService -> process() Completed validating headers & Started reading files data, uniqueId = {}", request.getUniqueId());

		Map<String, Map<String, List<String>>> sourceKeyDataChunkMap = documents.parallelStream().map(document -> {
			var fileNameKey = document.getSourceKey();
			var fileExtension = fileUtils.getFileExtension(document.getLocalPath());
			Map<String, List<ChunkDataMap>> temp = null;
			switch (fileExtension.toLowerCase()) {
			case "xlsx":
				temp = objectUtilsService.customReaderDynamicAutowireService.readLargeWorkbookData(
						document.getLocalPath(), fileNameKey, columnRelationMap, fileMetaDataMap, this.requestContext);
				break;
			case "csv":
				var fileMetadataInfo = fileMetaDataMap.get(fileNameKey);
				temp = objectUtilsService.customReaderDynamicAutowireService.readLargeCSVData(document.getLocalPath(),
						fileMetadataInfo, columnRelationMap.get(fileNameKey), this.requestContext);
				break;

			case "json":
				break;
			}
			if (temp == null)
				return null;

			return temp.entrySet().stream().collect(Collectors.toMap(Entry::getKey, entry -> {
				return (Map<String, List<String>>) entry.getValue().stream()
						.collect(Collectors.toMap(ChunkDataMap::getGroupIdentifierValue,
								ChunkDataMap::getChunkLocations, (a, b) -> a, HashMap::new));
			}));
		}).filter(Objects::nonNull).flatMap(m -> m.entrySet().stream())
				.collect(Collectors.toMap(Entry::getKey, Entry::getValue));

		log.info("SharedFilesJsonBatchProcessorService -> process() Completed reading files data, uniqueId = {}", request.getUniqueId());

		Supplier<Map<String, List<DataMap>>> formSupplier = () -> {
			var formJson = new JSONObject(request.getFormData());
			var formKey = "Form Data";
			return (Objects.nonNull(formJson) && !formJson.isEmpty())
					? objectUtilsService.customReaderDynamicAutowireService.readFormData(formJson,
							fileMetaDataMap.get(formKey), columnRelationMap.get(formKey), requestContext)
					: null;

		};

		var formMap = ValidationUtil.isHavingValue(request.getFormData()) ? formSupplier.get() : null;
		
		var tempRules = columnRelationMap.entrySet().parallelStream().flatMap(entry -> entry.getValue().stream())
				.filter(cr -> ValidationUtil.isHavingValue(cr.getDataExclusionRules()))
				.collect(Collectors.toMap(cr -> UUID.fromString(cr.getUuid()), ColumnRelation::getDataExclusionRules));

		Map<String, Map<String, byte[]>> sourceKeyChunkFilesMap = sourceKeyDataChunkMap.entrySet().parallelStream()
				.collect(Collectors.toMap(Entry::getKey, entry -> {
					return loadSourceKeyChunks(entry.getKey(), requestDir);
				}));

		log.info("SharedFilesJsonBatchProcessorService -> process() completed loading chunk data, uniqueId = {}",
				request.getUniqueId());
		
		log.info("SharedFilesJsonBatchProcessorService -> process() Started creating datasets for data, uniqueId = {}",
				request.getUniqueId());

		// Construct DataSets
		var dataSets = inputRules.stream().map(inputRule -> {
			DataSet<DataMap> temp = new DataSet<>();
			temp.setId(inputRule.getDataSetId());
			temp.setName(inputRule.getDataSetName());
			temp.setTargetFormatMap(new HashMap<UUID, TargetDataFormat>());
			temp.setBatchSize(inputRule.getBatchSize());

			try {
				var sourceKey = inputRule.getSourceKey();
				var sourceKeyDir = requestDir + "/" + sourceKey;
				var references = inputRule.getReferences();

				var currentDataSetDir = Paths.get(dataSetDir + "/" + inputRule.getDataSetId());
				var chunkDataSetDir = Paths.get(currentDataSetDir + "/chunks");

				if (Files.notExists(chunkDataSetDir)) {
					Files.createDirectories(chunkDataSetDir);
				}

				// var fileDataMetadataMap = readMetadata(fileMetaDataMap, requestDir);
				// var sourceDataMap = fileDataMetadataMap.get(sourceKey);

				var sourceDataMap = sourceKeyDataChunkMap.get(sourceKey);

				int[] count = { 0 };
				int[] masterCount = { 0 };
				var masterData = new ArrayList<DataMap>();

				// sourceDataMap.entrySet().parallelStream().forEach(entry -> {
				// var key = entry.getKey();
				// var values = entry.getValue();

				sourceDataMap.forEach((key, values) -> {

					var dataMap = new HashMap<String, List<DataMap>>();
					var skcfMap = sourceKeyChunkFilesMap.get(sourceKey);

					var sourceDataRows = values.parallelStream()
							.map(tempFileName -> {
								//log.info("key = {},  s = {}", key, tempFileName); 
								return readSourceRows(tempFileName, skcfMap.get(tempFileName + ".json"), key); 
							}).flatMap(List::stream).collect(Collectors.toList());

					// var sourceDataRows = values.parallelStream().map(tempFileName -> {
					// return readSourceRows(sourceKeyDir, tempFileName, key);
					// }).flatMap(List::stream).collect(Collectors.toList());

					dataMap.put(sourceKey, sourceDataRows);
					if(Objects.nonNull(references) && !references.isEmpty()) {
						var referenceDataRowsMap = references.parallelStream()
								.filter(reference -> !reference.getSourceKey().equalsIgnoreCase("Form Data"))
								.filter(reference -> Objects
										.nonNull(sourceKeyDataChunkMap.get(reference.getSourceKey()).get(key)))
								.collect(Collectors.toMap(Reference::getSourceKey, reference -> {
									var referenceKey = reference.getSourceKey();
									var referenceKeyDir = requestDir + "/" + referenceKey;
	
									// var referenceDataMap = fileDataMetadataMap.get(referenceKey);
									var referenceDataMap = sourceKeyDataChunkMap.get(referenceKey);
									var rkcfMap = sourceKeyChunkFilesMap.get(referenceKey);
									var rvalue = referenceDataMap.get(key);
	
									// return rvalue.parallelStream().map(tempFileName -> {
									// return readSourceRows(referenceKeyDir, tempFileName, key);
									// }).flatMap(List::stream).collect(Collectors.toList());
	
									return rvalue.parallelStream()
											.map(tempFileName -> {
												//log.info("key = {}, r ={}", key, tempFileName); 
												return readSourceRows(tempFileName, rkcfMap.get(tempFileName + ".json"), key);
											}).flatMap(List::stream).collect(Collectors.toList());								
	
								}));
	
						referenceDataRowsMap.values().removeIf(Objects::isNull);
	
						dataMap.putAll(referenceDataRowsMap);
					}

					if (Objects.nonNull(formMap)) {
						dataMap.putAll(formMap);
					}

					var dataSet = constructDataSet(inputRule, dataMap);

					if (Objects.nonNull(tempRules)) {
						applyExclusions(dataSet, tempRules);
					}
					
					//Construct Global values
					clauseMap.computeIfPresent(dataSet.getId(), (clauseKey, value) -> constructClause(value, dataSet.getData()));
					
					masterCount[0] += dataSet.getData().size();
					if ((masterData.size() + dataSet.getData().size()) <= inputRule.getBatchSize()) {
						masterData.addAll(dataSet.getData());
					} else {
						count[0] += 1;
						var dataSetFileName = new StringBuilder().append(dataSet.getName()).append("_").append(count[0])
								.append("_").append(masterData.size()).toString();
						// var dataSetFileName = dataSet.getName() + "_" + count[0] + "_" + masterDataMap.size();
						var file = Paths.get(chunkDataSetDir + "/" + dataSetFileName + ".json").toFile();

						var batch = new ArrayList<DataMap>(masterData);
						write(file, batch);
						masterData.clear();
						masterData.addAll(dataSet.getData());
					}
				});
				if (!masterData.isEmpty()) {
					count[0] += 1;
					var dataSetFileName = inputRule.getDataSetName() + "_" + count[0] + "_" + masterData.size();
					var file = Paths.get(chunkDataSetDir + "/" + dataSetFileName + ".json").toFile();
					var batch = new ArrayList<DataMap>(masterData);
					write(file, batch);
					masterData.clear();
				}
				log.info(
						"SharedFilesJsonBatchProcessorService -> process() Completed creating dataset, uniqueId = {}, dataSet = {}, Total Size = {}",
						request.getUniqueId(), inputRule.getDataSetName(), masterCount[0]);

			} catch (IOException e) {
				throw new ReaderException(e.getMessage(), e);
			}
			return temp;
		}).collect(Collectors.toList());

		log.info("SharedFilesJsonBatchProcessorService -> process() Completed creating datasets for data, uniqueId = {}",
				request.getUniqueId());

		this.updateRequest(request, RequestStatus.Read);		
		
		sourceKeyChunkFilesMap.clear();
		
		List<String> steps = null;
		steps = getProcessSteps(configurationData.getProcessSteps());
		if (steps == null) {
			steps = new ArrayList<String>();
			steps.add(ValidationType.client.getValidationType());
		}
		log.info("SharedFilesJsonBatchProcessorService -> process() Completed parsing steps, uniqueId = {}, steps = {}", request.getUniqueId(), steps);
		
		// Validations
		//String[] steps = new String[] {ValidationType.client.getValidationType()};
		for (String step : steps) {
			validate(dataSets, request, ValidationType.valueOf(step));
		}
		
		Map<String, Map<String, String>> dataSetDynamicClauseValuesMap = clauseMap.entrySet().parallelStream()
				.collect(Collectors.toMap(Entry::getKey, entry -> {			
					return entry.getValue()
					.stream()
					.collect(HashMap<String, String>::new, 
							(map, dynamicClause) -> map.put(dynamicClause.getName(), dynamicClause.getValue()), 
							HashMap<String, String>::putAll);
		}));
		
		transform(dataSets, request, dataSetRulesMap, dataSetDynamicClauseValuesMap);
		generate(dataSets, request, outputRulesJson);
		send(request, "output");
		this.updateRequest(request, RequestStatus.Completed);
		this.sendEmail("output");
		log.info("SharedFilesJsonBatchProcessorService -> process() Completed processing, uniqueId = {}", request.getUniqueId());
		return null;
	}

	public String getName(String sourceKey) {
		return sourceKey.replace(" ", "_").replace("{{", "$").replace("}}", "").trim();
	}
	
	@SuppressWarnings("unused")
	private String decompress1(byte[] sFile) {
		try(ByteArrayInputStream bais = new ByteArrayInputStream(sFile);
			GZIPInputStream gis = new GZIPInputStream(bais);
			ByteArrayOutputStream  baos = new ByteArrayOutputStream()) {
			//InputStreamReader isr = new InputStreamReader(gis, StandardCharsets.UTF_8);
			//BufferedReader reader = new BufferedReader(isr)
			byte[] buffer = new byte[1024];
			int len;
			while ((len = gis.read(buffer)) > 0) {
				baos.write(buffer, 0, len);				
			}
			return baos.toString(StandardCharsets.UTF_8);
		} catch (IOException e) {
			throw new ReaderException(e.getMessage(), e);
		}
	}
	
	@SuppressWarnings("unused")
	private String decompress(byte[] sFile) {
		try(ByteArrayInputStream bais = new ByteArrayInputStream(sFile);
			GZIPInputStream gis = new GZIPInputStream(bais);
				InputStreamReader isr = new InputStreamReader(gis, StandardCharsets.UTF_8);
				BufferedReader reader = new BufferedReader(isr)) {
			
			StringBuilder jsonBuilder = new StringBuilder();
			String line;
			while((line = reader.readLine()) != null) {
				jsonBuilder.append(line);
			}
			
			return jsonBuilder.toString();
		} catch (IOException e) {
			throw new ReaderException(e.getMessage(), e);
		}
	}
	private List<DataMap> readSourceRows(String fileName ,byte[] sFile, String key) throws ReaderException {
		try(JsonParser jsonParser = jsonFactory.createParser(sFile)) {			
			//var strJson = decompress(sFile);
			//var rootNode = objectUtilsService.objectMapper.readTree(strJson);
			var rootNode = objectUtilsService.objectMapper.readTree(jsonParser);
			var dataNode = rootNode.path(key);
			return objectUtilsService.objectMapper.convertValue(dataNode, new TypeReference<List<DataMap>>() {});
		} catch (IOException e) {
			log.info("SharedFilesJsonBatchProcessorService -> process() Received JSON request for processing, uniqueId = {}, fileName = {}",
					requestContext.getUniqueId(), fileName);
			throw new ReaderException(e.getMessage(), e);
		}
	}

	private Map<String, byte[]> loadSourceKeyChunks(String sourceKey, String requestDir) {
		var dir = Paths.get(requestDir + "/" + sourceKey);

		try (Stream<Path> paths = Files.walk(dir)) {
			// log.info("paths = {}", paths.parallel().filter(path ->
			// (!path.toFile().isDirectory() &&
			// path.getFileName().toString().startsWith(sourceKey+"_"))).map(path ->
			// path.getFileName().toString()).collect(Collectors.toList()));
			return paths.parallel().filter(
					path -> (!path.toFile().isDirectory() && path.getFileName().toString().startsWith(sourceKey + "_")))
					.collect(Collectors.toMap(path -> path.getFileName().toString(), path -> {
						try(var sReader = new FileReader(path.toFile())) {
							return objectUtilsService.objectMapper.writeValueAsBytes(objectUtilsService.objectMapper.readTree(sReader));
							//return Files.readAllBytes(path);
						} catch (IOException e) {
							throw new ReaderException(e.getMessage(), e);
						}
					}));
		} catch (IOException e) {
			throw new ReaderException(e.getMessage(), e);
		}
	}
	
	@SuppressWarnings("unused")
	private Map<String, Map<String, byte[]>> loadChunks(Map<String, Set<String>> fileChunks, String requestDir) throws IOException {		
		return fileChunks.entrySet().parallelStream().collect(Collectors.toMap(Map.Entry::getKey, entry -> {
			return entry.getValue().parallelStream().collect(Collectors.toMap(fileChunkName -> fileChunkName, fileChunkName -> {
				var sourceKeyDir = requestDir + "/" + entry.getKey();
				var fileName = fileChunkName + ".json";
				//new File(sourceKeyDir + "/" + fileName)
				try {
					return Files.readAllBytes(Path.of(sourceKeyDir + "/" + fileName));
				} catch (IOException e) {
					throw new ReaderException(e.getMessage(), e);
				}
			}));
		}));
	}
	
	
	@SuppressWarnings("unused")
	private List<DataMap> readSourceRows(String tempDir, String fileName, String key) {
		var sFile = new File(tempDir + "/" + fileName + ".json");
		try(var sReader = new FileReader(sFile)) { 
			var rootNode = objectUtilsService.objectMapper.readTree(sReader);
			var dataNode = rootNode.path(key);
			return objectUtilsService.objectMapper.convertValue(dataNode, new TypeReference<List<DataMap>>() {});
		} catch (IOException e) {
			throw new ReaderException(e.getMessage(), e);
		}
	}
	
	@SuppressWarnings("unused")
	private List<DataMap> readDatasetRows(String tempDir, String fileName) throws IOException {
		var sFile = new File(tempDir + "/" + fileName + ".json");
		try(var sReader = new FileReader(sFile)) {			
			return objectUtilsService.objectMapper.readValue(sReader, new TypeReference<List<DataMap>>() {});			
		} 
	}
	
	@SuppressWarnings("unused")
	private Map<String,Map<String, List<String>>> readMetadata(Map<String,FileMetaData> fileMetaDataMap, String requestDir) throws IOException {
		
		var map = new HashMap<String, Map<String, List<String>>>();
		
		for (Map.Entry<String, FileMetaData> entry : fileMetaDataMap.entrySet()) {
			//String key = entry.getKey();
			FileMetaData fileMetadata = entry.getValue();
			
			var sourceKey = fileMetadata.getSourceKey();
			if(!sourceKey.equalsIgnoreCase("Form Data")) {
				var sourceKeyDir = requestDir + "/" + sourceKey;			
				var fileName = "meta.json";
				var sourceMetaFile = new File(sourceKeyDir + "/" + fileName);
				var sourceMetaReader = new FileReader(sourceMetaFile);
				Map<String, List<String>> sourceMetaFileMap = objectUtilsService.objectMapper.readValue(sourceMetaReader, new TypeReference<Map<String, List<String>>>() {});
				Map<String, List<String>> sourceDataMap = new HashMap<>();
				sourceMetaFileMap.forEach((key, value) -> value.forEach(item -> sourceDataMap.computeIfAbsent(item, k -> new ArrayList<>()).add(key)));
				
				map.put(entry.getKey(), sourceDataMap);
			}
		}
		return map;		
	}

}
