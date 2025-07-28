package com.adp.esi.digitech.file.processing.processor.service;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.file.processing.autowire.service.CustomProcessorDynamicAutowireService;
import com.adp.esi.digitech.file.processing.exception.ConfigurationException;
import com.adp.esi.digitech.file.processing.exception.GenerationException;
import com.adp.esi.digitech.file.processing.exception.ProcessException;
import com.adp.esi.digitech.file.processing.exception.ReaderException;
import com.adp.esi.digitech.file.processing.exception.TransformationException;
import com.adp.esi.digitech.file.processing.exception.ValidationException;
import com.adp.esi.digitech.file.processing.file.service.IFileService;
import com.adp.esi.digitech.file.processing.model.DataMap;
import com.adp.esi.digitech.file.processing.model.RequestPayload;

import lombok.extern.slf4j.Slf4j;

@Service
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class SharedFilesAsyncProcessorService extends AbstractAsyncProcessorService<Void> {

	//IFileService iFilesService;
	
	@Autowired
	CustomProcessorDynamicAutowireService customProcessorDynamicAutowireService;

	public void constructDefaults() {
		super.constructDefaults(requestContext.getBu(), requestContext.getPlatform(), requestContext.getDataCategory());
	}

	@Override
	public Void process(RequestPayload request) throws IOException, ReaderException, ConfigurationException,
			ValidationException, TransformationException, GenerationException, ProcessException {
		log.info("SharedFilesAsyncProcessorService -> process() Received JSON request for processing, uniqueId = {}, request = {}", request.getUniqueId(), request);
		
		try {
			customProcessorDynamicAutowireService.processAsync(SharedFilesFluxJsonBatchProcessorServiceV2.class, request, requestContext);
			request.getDocuments().parallelStream().forEach(document -> {
				this.clean(document.getLocation(),request.getUniqueId());
			});
		} catch (Exception  e) {
			e.printStackTrace();
			log.error("SharedFilesAsyncProcessorService -> process() Process execution failed with erros, uniqueId = {}, errors = {}", request.getUniqueId(), e.getMessage());
			handleError(e);
			throw new ProcessException("Process execution failed with erros", e);
		} finally {
			clean(largeRequestFilePath + requestContext.getRequestUuid());
		}
		
		//this.updateRequest(request, RequestStatus.Started);

		//this.iFilesService = objectUtilsService.objectProviderSharedFilesService.getObject(requestContext, TargetLocation.SharedDrive);
		
		//var requestDir = largeRequestFilePath + requestContext.getRequestUuid();
		//var sourceDir  = requestDir + "/source";
		//var dataSetDir = requestDir + "/dataset";
		//this.constructDefaults();
		
		//Map<String, List<ValidationRule>> validationRulesMap = objectUtilsService.dataStudioConfigurationService.findAllValidationRulesGroupBy(requestContext.getBu(), requestContext.getPlatform(), requestContext.getDataCategory());
		
		//String inputRulesJson = configurationData.getInputRules();
		//String outputRulesJson = configurationData.getOutputFileRules();
		//String filesInfoJson = configurationData.getFilesInfo();
		//String datasetRulesJson = configurationData.getDataRules();

		//List<InputRule> inputRules = this.getInputRules(inputRulesJson);
		
		//Map<String, FileMetaData> fileMetaDataMap = ValidationUtil.isHavingValue(filesInfoJson) ? this.getFileMetaDataMap(filesInfoJson) : this.getFileMetaDataMap(inputRules);
		
		//List<DataSetRules> dataSetRules = this.getDataSetRules(datasetRulesJson);
		
		//var dataSetRulesMap = Objects.nonNull(dataSetRules)? dataSetRules.stream().collect(Collectors.toMap(DataSetRules::getDataSetId, Function.identity())) : null;
		
		
		//var documents = load(request, ProcessType.chunks);
		
		/*
		var sourceKeyChunkFilesMap1 = documents.parallelStream().collect(Collectors.toMap(Document::getSourceKey, document -> {
			//var requestDir = largeRequestFilePath + requestContext.getRequestUuid();
			return loadSourceKeyChunks(document.getSourceKey(), requestDir);
		}));
		*/
		
		//var sourceKeyChunkFilesMap = loadChunks(sourceKeyChunkMap, largeRequestFilePath + requestContext.getRequestUuid());
			
		

		/*
		var dataSets = inputRules.stream().map(inputRule -> {
            DataSet<DataMap> temp = new DataSet<>();
            temp.setId(inputRule.getDataSetId());
            temp.setName(inputRule.getDataSetName());
            temp.setTargetFormatMap(new HashMap<UUID, TargetDataFormat>());
            
            try {
                var sourceKey = inputRule.getSourceKey();					
                var sourceKeyDir = requestDir + "/" + sourceKey;			
                var references = inputRule.getReferences();			
            
                var dir = Paths.get(requestDir + "/" + inputRule.getDataSetId());
            	
            	if(Files.notExists(dir)) {
            		Files.createDirectories(dir);
            	}
                //var fileDataMetadataMap = readMetadata(fileMetaDataMap, requestDir);				
                //var sourceDataMap = fileDataMetadataMap.get(sourceKey);
            	
            	var sourceDataMap = sourceKeyDataChunkMap.get(sourceKey);
                
                int[] count = {0};
                int [] masterCount = {0};
                var masterData = new ArrayList<DataMap>();
                
                Flux.fromIterable(sourceDataMap.entrySet()).map(entry -> {
                	
                	var key = entry.getKey();
                	//log.info("Calling key = {}", key);
                	var values = entry.getValue();
                	
                	 var dataMap = new HashMap<String, List<DataMap>>();
                     
                     var skcfMap = sourceKeyChunkFilesMap.get(sourceKey);
                     
                     var sourceDataRows = values.parallelStream()
                             .map(tempFileName -> readSourceRows(skcfMap.get(tempFileName + ".json"), key))
                             .flatMap(List::stream)
                             .collect(Collectors.toList());
                     
                     //var sourceDataRows = values.parallelStream().map(tempFileName -> {						
                     //    return readSourceRows(sourceKeyDir, tempFileName, key);											 
                    // }).flatMap(List::stream).collect(Collectors.toList());
                     
                     
                     dataMap.put(sourceKey, sourceDataRows);
                     
                     var referenceDataRowsMap = references.parallelStream()
                     .filter(reference -> !reference.getSourceKey().equalsIgnoreCase("Form Data"))
                     .filter(reference -> Objects.nonNull(sourceKeyDataChunkMap.get(reference.getSourceKey()).get(key)))
                     .collect(Collectors.toMap(Reference::getSourceKey, reference -> {
                         var referenceKey = reference.getSourceKey();
                         var referenceKeyDir = requestDir + "/" + referenceKey;
                         
                         //var referenceDataMap = fileDataMetadataMap.get(referenceKey);
                         //var referenceData
                         var referenceDataMap = sourceKeyDataChunkMap.get(referenceKey);
 						var rkcfMap = sourceKeyChunkFilesMap.get(referenceKey);						
 						var rvalue = referenceDataMap.get(key);						
 						
 						return rvalue.parallelStream()
 								.map(tempFileName -> readSourceRows(rkcfMap.get(tempFileName + ".json"), key))
 								.flatMap(List::stream)
 								.collect(Collectors.toList());
                     }));
                     referenceDataRowsMap.values().removeIf(Objects::isNull);
 					
 					dataMap.putAll(referenceDataRowsMap);
 										
 					if(Objects.nonNull(formMap)) {						
 						dataMap.putAll(formMap);	
 					}
 					
 					var dataSet = constructDataSet(inputRule, dataMap);
 					
 					if(Objects.nonNull(tempRules)) {
 						applyExclusions(dataSet, tempRules);
 					}
                   return dataSet.getData();  
                })
                .doOnNext(rows -> {
                	
					masterCount[0] += rows.size();
					if ((masterData.size() + rows.size()) <= inputRule.getBatchSize()) {
						masterData.addAll(rows);
					} else {
						count[0] += 1;
						var dataSetFileName = new StringBuilder()
								.append(inputRule.getDataSetName())
								.append("_")
								.append(count[0])
								.append("_")
								.append(rows.size())
								.toString();
						var file = Paths.get(dir + "/" + dataSetFileName + ".json").toFile();
					
						var batch = new ArrayList<DataMap>(masterData);
						CompletableFuture.runAsync(() -> {
							try {								
								objectUtilsService.objectMapper.writeValue(file, batch);
								masterData.clear();
							} catch (IOException e) {
								throw new ReaderException(e.getMessage(), e);
							}
							masterData.addAll(rows);
						});
					}
					log.info("recevied rows = {}, master = {}", rows.size(), masterData.size());
                }).blockLast();
                
               
				log.info("SharedFilesProcessorService -> process() Completed creating dataset, uniqueId = {}, dataSet = {}, Total Size = {}", request.getUniqueId(), inputRule.getDataSetName(), masterCount[0]);
				
			} catch (IOException e) {
				throw new ReaderException(e.getMessage(), e);
			} 
            return temp;
        }).collect(Collectors.toList());
		*/
		
		//if(true) return null;
		
		//CAM Validation
		/*
		var isCAM = false;
		if(isCAM) {
			//Need to enhance more			
			dataSets.parallelStream().forEach(temp -> {	
				var dir = Paths.get(requestDir + "/" + temp.getId());
				try(Stream<Path> paths = Files.list(dir)){
					paths.parallel().forEach(path -> {
						try {
							var sReader = new FileReader(path.toFile());
							var dataSetRows = objectUtilsService.objectMapper.readValue(sReader, new TypeReference<List<DataMap>>() {});
							
							DataSet<DataMap> dataSet = new DataSet<>();
							dataSet.setId(temp.getId());
							dataSet.setName(temp.getName());
							dataSet.setData(dataSetRows);
							
							this.validate(dataSet, ValidationType.CAM);		
							
						} catch (IOException e) {
							
						}
						
					});
				} catch (IOException e) {
					throw new ValidationException(e.getMessage(), e);
				}				
			});	
			log.info("SharedFilesProcessorService -> process() Completed validating CAM validations, uniqueId = {}", request.getUniqueId());
		}
		*/
		
		//
		
		
		return null;
	}

	public void setiFilesService(IFileService iFilesService) {
		this.iFilesService = iFilesService;
	}
	
	@Override
	public Map<String, List<DataMap>> read(RequestPayload data) throws ReaderException {
		return null;
	}
}