package com.adp.esi.digitech.file.processing.processor.service;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import com.adp.esi.digitech.file.processing.enums.RelationshipType;
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
import com.adp.esi.digitech.file.processing.model.Document;
import com.adp.esi.digitech.file.processing.model.ErrorData;
import com.adp.esi.digitech.file.processing.model.RequestPayload;
import com.adp.esi.digitech.file.processing.model.Row;
import com.adp.esi.digitech.file.processing.util.FileUtils;
import com.adp.esi.digitech.file.processing.util.FileValidationUtils;
import com.adp.esi.digitech.file.processing.util.ValidationUtil;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

@Service
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class SharedFilesFluxJsonBatchProcessorServiceV2 extends AbstractAsyncProcessorService<Void> {

	@Autowired
	FileUtils fileUtils;
	
	@Autowired
	FileValidationUtils fileValidationUtils;

	@Value("${large.request.file.path}")
	String largeRequestFilePath;
	
	private Map<String, List<UUID>> datasetFieldsMap = new HashMap<>();

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
				var excelFileMetaDataMap = getFileMetaDataMap(fileMetaDataMap, fileNameKey);	
				temp = objectUtilsService.customReaderDynamicAutowireService.readLargeWorkbookData(
						document.getLocalPath(), fileNameKey, columnRelationMap, excelFileMetaDataMap, this.requestContext);
				break;
			case "csv":
				var fileMetadataInfo = fileMetaDataMap.get(fileNameKey);
				temp = objectUtilsService.customReaderDynamicAutowireService.readLargeCSVData(document.getLocalPath(),
						fileMetadataInfo, columnRelationMap.get(fileNameKey), this.requestContext);
				break;

			case "txt": 						
			case "dat":
				var txtFileMetadataInfo = fileMetaDataMap.get(fileNameKey);
				var txtColumnRelations = columnRelationMap.entrySet().stream().filter(entry -> entry.getKey().startsWith(fileNameKey)).flatMap(entry -> entry.getValue().stream()).collect(Collectors.toList());
				temp = objectUtilsService.customReaderDynamicAutowireService.readLargeTXTData(document.getLocalPath(), 
						txtFileMetadataInfo, txtColumnRelations, this.requestContext);					
				
				break;
			case "xml":
                var fileMetadataInfoXml = fileMetaDataMap.get(fileNameKey);
                var xmlColumnRelations = columnRelationMap.values().stream().flatMap(List::stream).filter(cr -> cr.getSourceKey().startsWith(fileNameKey)).collect(Collectors.toList());
                temp = objectUtilsService.customReaderDynamicAutowireService.readLargeXMLData(document.getLocalPath(), 
                		fileMetadataInfoXml, xmlColumnRelations, this.requestContext);                    
               
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
		}).filter(Objects::nonNull).flatMap(m -> m.entrySet().stream()).collect(Collectors.toMap(Entry::getKey, Entry::getValue));

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

		log.info("SharedFilesJsonBatchProcessorService -> process() completed loading chunk data, uniqueId = {}", request.getUniqueId());
		
		this.updateRequest(request, RequestStatus.Read);
		
		log.info("SharedFilesJsonBatchProcessorService -> process() Started creating datasets for data, uniqueId = {}",	request.getUniqueId());
		
		List<String> steps = getProcessSteps(configurationData.getProcessSteps());
		
		log.info("SharedFilesJsonBatchProcessorService -> process() Completed parsing steps, uniqueId = {}, steps = {}", request.getUniqueId(), steps);
		
		
		
		var dataSets = new ArrayList<DataSet<DataMap>>();
		columnsToValidateMap = getSourceColumnsToValidate(inputRules);
		var orderedInputRules = getOrderedRules(inputRules);
		
		orderedInputRules.forEach((order, tempOrderedInputRules) -> {
			log.info("SharedFilesJsonBatchProcessorService -> process() Started Processing uniqueId = {},  order = {}", request.getUniqueId(), order);
			
			
			if(order == 0) {
				validateOrder(tempOrderedInputRules);
			}			
			// Construct DataSets
			var orderedDataSets = tempOrderedInputRules.stream().map(inputRule -> {
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

					var sourceDataMap = sourceKeyDataChunkMap.get(sourceKey);

					int[] count = { 0 };
					int[] masterCount = { 0 };
					var masterData = new ArrayList<DataMap>();

					sourceDataMap.forEach((key, values) -> {

						var dataMap = new HashMap<String, List<DataMap>>();
						var skcfMap = sourceKeyChunkFilesMap.get(sourceKey);

						var sourceDataRows = Flux.fromIterable(values)
								.flatMap(tempFileName -> Flux.just(tempFileName)
															 .publishOn(Schedulers.boundedElastic()).
															 map(fileName -> readSourceRows(fileName, skcfMap.get(tempFileName + ".json"), key)))
								.flatMap(list -> Flux.fromIterable(list))
								.collectList()
								.block();

						dataMap.put(sourceKey, sourceDataRows);
						if(Objects.nonNull(references) && !references.isEmpty()) {
							var referenceDataRowsMap = references.parallelStream()
									.filter(reference -> !reference.getSourceKey().equalsIgnoreCase("Form Data"))
									.filter(reference -> Objects.nonNull(sourceKeyDataChunkMap.get(reference.getSourceKey()).get(key)))
									.collect(Collectors.toMap(Reference::getSourceKey, reference -> {
										var referenceKey = reference.getSourceKey();
										var referenceKeyDir = requestDir + "/" + referenceKey;	
										
										var referenceDataMap = sourceKeyDataChunkMap.get(referenceKey);
										var rkcfMap = sourceKeyChunkFilesMap.get(referenceKey);
										var rvalues = referenceDataMap.get(key);									
		
										return Flux.fromIterable(rvalues)
												.flatMap(tempFileName -> Flux.just(tempFileName)
																		.publishOn(Schedulers.boundedElastic())
																		.map(fileName -> readSourceRows(fileName, rkcfMap.get(tempFileName + ".json"), key)))
												.flatMap(list -> Flux.fromIterable(list))
												.collectList()
												.block();								
		
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
							
							log.info("count = {}, dataSet = {}, size = {}", count[0], dataSet.getName(), batch.size());
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
					log.info("SharedFilesJsonBatchProcessorService -> process() Completed creating dataset, uniqueId = {}, dataSet = {}, Total Size = {}",
							request.getUniqueId(), inputRule.getDataSetName(), masterCount[0]);

				} catch (IOException e) {
					throw new ReaderException(e.getMessage(), e);
				}
				return temp;
			}).collect(Collectors.toList());
			
			// Validations			
			for (String step : steps) {
				try {
					validate(orderedDataSets, request, ValidationType.valueOf(step));
				} catch (IOException e) {
					throw new ProcessException(e.getMessage(), e);
				}
			}
			
			Map<String, Map<String, String>> dataSetDynamicClauseValuesMap = clauseMap.entrySet().parallelStream()
					.collect(Collectors.toMap(Entry::getKey, entry -> {			
						return entry.getValue()
						.stream()
						.collect(HashMap<String, String>::new, 
								(map, dynamicClause) -> map.put(dynamicClause.getName(), dynamicClause.getValue()), 
								HashMap<String, String>::putAll);
			}));
			
			transform(orderedDataSets, request, dataSetRulesMap, dataSetDynamicClauseValuesMap);			
			
			dataSets.addAll(orderedDataSets);
			if(orderedInputRules.size() > 1 && order < (orderedInputRules.size()-1)) {				
				orderedDataSets.forEach(orderedDataSet -> {
					try {
						var dataSetId = orderedDataSet.getId();
						var transformDir = Paths.get(dataSetDir + "/" + dataSetId + "/transform");
						var dir = Paths.get(requestDir + "/" + dataSetId);
						var fileMetadata = fileMetaDataMap.get(dataSetId);
						AtomicInteger fileCounter = new AtomicInteger();
						if(Files.notExists(dir)) {
							Files.createDirectories(dir);
						}
						Map<String, List<String>> metaData = new HashMap<>();
						Files.list(transformDir).filter(path -> path.toFile().isFile()).forEach(path -> {
							try {
								var sReader = new FileReader(path.toFile());
								var rows = objectUtilsService.objectMapper.readValue(sReader, new TypeReference<List<Row>>() {});
								
								
								
								var dataMapList =  rows.parallelStream().map(row -> {
									var columns =row.getColumns().entrySet().parallelStream().collect(HashMap<UUID,String>::new, 
											(map,entrySet) -> map.put(entrySet.getKey(), entrySet.getValue().getTargetValue()), 
											HashMap<UUID,String>::putAll);	
									var dataMapObj = new DataMap();
									dataMapObj.setColumns(columns);							
									return dataMapObj;
								}).collect(Collectors.toList());					        	

								var batchData = new ArrayList<>(dataMapList);				
								var fileName = dataSetId + "_" + fileCounter.incrementAndGet();
								var groupRows = batchData.parallelStream().collect(Collectors.groupingBy(row -> row.getColumns().get(UUID.fromString(fileMetadata.getGroupIdentifier()))));
								var keys = groupRows.keySet().stream().collect(Collectors.toList());				
								
								var file = Paths.get(dir + "/" + fileName + ".json").toFile();
								write(file, groupRows);
								metaData.put(fileName, keys);
								rows.clear();
								dataMapList.clear();
							} catch (IOException e) {
								throw new ProcessException(e.getMessage(), e);
							}
						});
						
						Map<String, List<String>> sourceDataMap = new HashMap<>();
						metaData.forEach((key, value) -> value.forEach(item -> sourceDataMap.computeIfAbsent(item, k -> new ArrayList<>()).add(key)));
						sourceKeyDataChunkMap.put(dataSetId, sourceDataMap);
						
						var chunks = loadSourceKeyChunks(dataSetId, requestDir);
						sourceKeyChunkFilesMap.put(dataSetId, chunks);
						
					} catch (IOException e) {
						throw new ProcessException(e.getMessage(), e);
					}
				});				
			}
			log.info("SharedFilesJsonBatchProcessorService -> process() Completed Processing uniqueId = {},  order = {}", request.getUniqueId(), order);
		});

		

		log.info("SharedFilesJsonBatchProcessorService -> process() Completed creating datasets for data, uniqueId = {}", request.getUniqueId());

		sourceKeyChunkFilesMap.clear();
		
		generate(dataSets, request, outputRulesJson);
		send(request, "output");
		this.updateRequest(request, RequestStatus.Completed);
		this.sendEmail("output");
		log.info("SharedFilesJsonBatchProcessorService -> process() Completed processing, uniqueId = {}", request.getUniqueId());
		return null;
	}
	
	@Override
	public List<Document> load(RequestPayload request, ProcessType processType) throws ValidationException {

		var requestDir = largeRequestFilePath + requestContext.getRequestUuid();
		List<ErrorData> errors = new ArrayList<>();

		var documents = Flux.fromIterable(request.getDocuments())
							.flatMap(doc -> Flux.just(doc)
								.publishOn(Schedulers.boundedElastic())
								.map(document -> {
									try {
										var sourceDir = requestDir + "/" + document.getSourceKey();
										MultipartFile file = iFilesService.getFile(document.getLocation(), appCode);
										var extension = fileUtils.getFileExtension(file.getOriginalFilename());
										switch (processType) {
										case chunks:
											var fileName = getName(document.getSourceKey()) + "." + extension;
											document.setLocalPath(sourceDir + "/" + fileName);
											iFilesService.copyToLocal(sourceDir, fileName, file);
											break;
										default:
											document.setFile(file);
										}
						
										return document;
									} catch (IOException e) {
										log.error(
												"AbstractFilesProcessorService -> load() Failed to get file from given location, uniqueId = {}, sourceKey = {}, location = {}, errorMessage = {}",
												request.getUniqueId(), document.getSourceKey(), document.getLocation(), e.getMessage());
										var error = new ErrorData(document.getSourceKey(), e.getMessage());
										errors.add(error);
										return null;
									}
								})).filter(Objects::nonNull).collectList().block();

		if (documents.size() != request.getDocuments().size()) {
			var validationException = new ValidationException("Request Validation - Failed while reading file from the Network location");
			validationException.setErrors(errors);
			validationException.setRequestContext(requestContext);
			throw validationException;
		}

		return documents;
	}
	
	@Override
	public DataSet<DataMap> constructDataSet(InputRule inputRule, Map<String, List<DataMap>> dataMap) {
		var sourceKey= inputRule.getSourceKey();
		var sourceData = dataMap.get(sourceKey);
		var tempData = sourceData; //.stream().map(SerializationUtils::clone).collect(Collectors.toList());
		
		if(tempData == null || tempData.isEmpty()) {
			var validationException = new ValidationException("No data found in source");
			var errors = List.of(new ErrorData(sourceKey, "Empty data"));
			validationException.setErrors(errors);
			validationException.setRequestContext(requestContext);
			throw validationException;
			
		}
		var dataSetName = inputRule.getDataSetName();		
		var dataSetId = inputRule.getDataSetId();			
		var references = inputRule.getReferences();
		if(references != null && !references.isEmpty()) {
	
			for (Reference reference : references) {
				
				var relationship = reference.getRelationship();					
				relationship = Objects.nonNull(relationship) ? relationship : RelationshipType.OnetoOne;
				
				var rsourceKey= reference.getSourceKey();		
				
				var referenceData = dataMap.get(rsourceKey);	
				var keyIdentifiers = reference.getKeyIdentifiers();			
				var referenceType = reference.getType();
				
				var referenceDataMap = (Objects.nonNull(referenceData) && Objects.nonNull(keyIdentifiers) && !keyIdentifiers.isEmpty()) ? 
						referenceData.parallelStream()
						.collect(Collectors.toMap(rRow -> keyIdentifiers.stream()
																		.map(keyIdentifier -> String.valueOf(rRow.getColumns().get(UUID.fromString(keyIdentifier.getReferenceIdentifier()))))
																		.collect(Collectors.joining("_"))
												, Function.identity(), (existing, replacement) -> existing)) : null;
				
				 var emptyCollumnMap = new HashMap<UUID, String>();
					if("dataset".equalsIgnoreCase(referenceType)) {
						if(Objects.nonNull(referenceData) && !referenceData.isEmpty()) {
							var referenceFirstRow = referenceData.get(0);
							var rColumns = referenceFirstRow.getColumns();
							rColumns.forEach((key, value) -> {
								emptyCollumnMap.put(key, null);
							});	
						} else {
							var columnUUIDList = datasetFieldsMap.get(rsourceKey);
							if(Objects.nonNull(columnUUIDList)) {
								columnUUIDList.forEach(key -> {
									emptyCollumnMap.put(key, null);
								});
							}
						}
					} else {
						var rColumnRealtions = columnRelationMap.get(rsourceKey);					   
					   
					    rColumnRealtions.forEach(relation -> {
					    	emptyCollumnMap.put(UUID.fromString(relation.getUuid()), null);
					    });
					}
				
				switch (relationship) {
					case OnetoOne:
						
						tempData = Flux.fromIterable(tempData)
							.publishOn(Schedulers.boundedElastic())
							.map(sRow -> {								
							if(Objects.nonNull(referenceDataMap)) {								
								var sKeyIdentifier = keyIdentifiers.stream().map(keyIdentifier -> String.valueOf(sRow.getColumns().get(UUID.fromString(keyIdentifier.getParentIdentifier())))).collect(Collectors.joining("_"));
								
								if(referenceDataMap.containsKey(sKeyIdentifier)) {
									var tempRow = referenceDataMap.get(sKeyIdentifier);//SerializationUtils.clone();
									sRow.getColumns().putAll(tempRow.getColumns());
									return sRow;
								}
							}
							sRow.getColumns().putAll(emptyCollumnMap);
							return sRow;
						}).collectList().block();
						
						break;	
					case AlltoOne:
						
						var rowNumber = reference.getDataRowNumber();
						var row = Objects.isNull(referenceData) || referenceData.isEmpty() ? new DataMap(emptyCollumnMap) : referenceData.size() >= rowNumber ?	referenceData.get(rowNumber-1) : referenceData.get(0);				
						
						tempData = Flux.fromIterable(tempData)
								.publishOn(Schedulers.boundedElastic()).map(sRow -> {
							var tempRow = row;
							sRow.getColumns().putAll(tempRow.getColumns());
							return sRow;
						}).collectList().block();
						
						break;						
					case ManytoOne: case OnetoMany:
						break;
				}		
			}
		}
		DataSet<DataMap> dataSet = new DataSet<>();
		dataSet.setId(dataSetId);
		dataSet.setName(dataSetName);			
		dataSet.setData(tempData);	
		
		if(Objects.nonNull(tempData) && !tempData.isEmpty() && !datasetFieldsMap.containsKey(dataSetId)) {
			var tempRow = tempData.get(0);
			var list = new ArrayList<UUID>();
			var keys = tempRow.getColumns().keySet();
			list.addAll(keys);			
			datasetFieldsMap.put(dataSetId, list);
		}
		
		
		return dataSet;
	}

	public String getName(String sourceKey) {
		return sourceKey.replace(" ", "_").replace("{{", "$").replace("}}", "").trim();
	}
	
	
	
	private List<DataMap> readSourceRows(String fileName ,byte[] sFile, String key) throws ReaderException {
		try(JsonParser jsonParser = jsonFactory.createParser(sFile)) {			
			var rootNode = objectUtilsService.objectMapper.readTree(jsonParser);
			var dataNode = rootNode.path(key);
			return objectUtilsService.objectMapper.convertValue(dataNode, new TypeReference<List<DataMap>>() {});
		} catch (IOException e) {
			log.error("SharedFilesJsonBatchProcessorService -> process() Received JSON request for processing, uniqueId = {}, fileName = {}",
					requestContext.getUniqueId(), fileName);
			throw new ReaderException(e.getMessage(), e);
		}
	}

	private Map<String, byte[]> loadSourceKeyChunks(String sourceKey, String requestDir) {
		var dir = Paths.get(requestDir + "/" + sourceKey);

		try (Stream<Path> paths = Files.walk(dir)) {
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
	
	public void write(File file, Object value) {
		try {
			objectUtilsService.objectMapper.writeValue(file, value);
		} catch (IOException e) {
			throw new ProcessException(e.getMessage(), e);
		}
	}

}
