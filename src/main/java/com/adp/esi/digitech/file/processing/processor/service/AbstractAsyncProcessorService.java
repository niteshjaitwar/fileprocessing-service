package com.adp.esi.digitech.file.processing.processor.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.web.multipart.MultipartFile;

import com.adp.esi.digitech.file.processing.ds.config.model.DataSetRules;
import com.adp.esi.digitech.file.processing.ds.config.model.DynamicClause;
import com.adp.esi.digitech.file.processing.ds.config.model.FileMetaData;
import com.adp.esi.digitech.file.processing.ds.model.ColumnRelation;
import com.adp.esi.digitech.file.processing.enums.ProcessType;
import com.adp.esi.digitech.file.processing.enums.RequestStatus;
import com.adp.esi.digitech.file.processing.enums.Status;
import com.adp.esi.digitech.file.processing.enums.TargetLocation;
import com.adp.esi.digitech.file.processing.enums.ValidationType;
import com.adp.esi.digitech.file.processing.exception.ConfigurationException;
import com.adp.esi.digitech.file.processing.exception.DataValidationException;
import com.adp.esi.digitech.file.processing.exception.GenerationException;
import com.adp.esi.digitech.file.processing.exception.MetadataValidationException;
import com.adp.esi.digitech.file.processing.exception.ProcessException;
import com.adp.esi.digitech.file.processing.exception.ReaderException;
import com.adp.esi.digitech.file.processing.exception.TransformationException;
import com.adp.esi.digitech.file.processing.exception.ValidationException;
import com.adp.esi.digitech.file.processing.file.service.IFileService;
import com.adp.esi.digitech.file.processing.generator.service.LargeCSVGeneratorService;
import com.adp.esi.digitech.file.processing.generator.service.LargeExcelGeneratorService;
import com.adp.esi.digitech.file.processing.generator.service.LargeStAXXMLGeneratorService;
import com.adp.esi.digitech.file.processing.generator.service.LargeTextGeneratorService;
import com.adp.esi.digitech.file.processing.model.ApiResponse;
import com.adp.esi.digitech.file.processing.model.Column;
import com.adp.esi.digitech.file.processing.model.DataMap;
import com.adp.esi.digitech.file.processing.model.DataSet;
import com.adp.esi.digitech.file.processing.model.Document;
import com.adp.esi.digitech.file.processing.model.ErrorData;
import com.adp.esi.digitech.file.processing.model.ErrorResponse;
import com.adp.esi.digitech.file.processing.model.FPSRequest;
import com.adp.esi.digitech.file.processing.model.RequestContext;
import com.adp.esi.digitech.file.processing.model.RequestPayload;
import com.adp.esi.digitech.file.processing.model.Row;
import com.adp.esi.digitech.file.processing.model.SharedFile;
import com.adp.esi.digitech.file.processing.notification.model.EmailNotificationData;
import com.adp.esi.digitech.file.processing.util.FileUtils;
import com.adp.esi.digitech.file.processing.util.FileValidationUtils;
import com.adp.esi.digitech.file.processing.util.ValidationUtil;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
public abstract class AbstractAsyncProcessorService<T> extends AbstractProcessorService<T> {

	IFileService iFilesService;

	@Autowired
	FileUtils fileUtils;
	
	@Autowired
	FileValidationUtils fileValidationUtils;

	@Value("${large.request.file.path}")
	String largeRequestFilePath;
	
	@Autowired
	JsonFactory jsonFactory;

	public List<Document> load(RequestPayload request, ProcessType processType) throws ValidationException {

		var requestDir = largeRequestFilePath + requestContext.getRequestUuid();
		List<ErrorData> errors = new ArrayList<>();

		var documents = request.getDocuments().parallelStream().map(document -> {
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
		}).filter(Objects::nonNull).collect(Collectors.toList());

		if (documents.size() != request.getDocuments().size()) {
			var validationException = new ValidationException(
					"Request Validation - Failed while reading file from the Network location");
			validationException.setErrors(errors);
			validationException.setRequestContext(requestContext);
			throw validationException;
		}

		return documents;
	}

	public String getName(String sourceKey) {
		return sourceKey.replace(" ", "_").replace("{{", "$").replace("}}", "").trim();
	}

	private void updateRequest(RequestContext requestContext, String errorType, String errorDetails,
			List<ErrorData> errors) {
		FPSRequest dataProcessingRequest = new FPSRequest();
		dataProcessingRequest.setUniqueId(requestContext.getUniqueId());
		dataProcessingRequest.setUuid(requestContext.getRequestUuid());
		dataProcessingRequest.setBu(requestContext.getBu());
		dataProcessingRequest.setPlatform(requestContext.getPlatform());
		dataProcessingRequest.setDataCategory(requestContext.getDataCategory());
		dataProcessingRequest.setStatus("Failed");
		dataProcessingRequest.setErrorType(errorType);

		errorDetails = ValidationUtil.isHavingValue(errorDetails) ? errorDetails : "";

		if (errors != null)
			try {
				String errorJson = new ObjectMapper().writeValueAsString(errors);
				errorDetails = errorDetails.concat(" - errors").concat(errorJson);
			} catch (IOException e) {
				log.error("AbstractFilesProcessorService -> updateRequest() Failed to convert errors to json, uniqueId = {}, errorDetails = {}", requestContext.getUniqueId(), e.getMessage());
				
			}
		dataProcessingRequest.setErrorDetails(errorDetails);
		objectUtilsService.fpsRequestService.update(dataProcessingRequest);
	}

	private void sendExceptionEmail(RequestContext requestContext, String message, Throwable cause,
			List<ErrorData> errors) {
		EmailNotificationData emailNotificationData = new EmailNotificationData();
		emailNotificationData.setRequestContext(requestContext);
		emailNotificationData.setRootError(message);
		if (Objects.nonNull(errors)) {
			emailNotificationData.setErrors(errors);
		}
		if (Objects.nonNull(cause)) {
			emailNotificationData.setRootCasue(cause.getMessage());
		}
		try {
			objectUtilsService.emailNotificationService.sendExceptionEmail("", emailNotificationData);
		} catch (IOException ioException) {
			log.error("AbstractFilesProcessorService -> sendExceptionEmail() Failed to send exception email, uniqueId = {}, errorDetails = {}", requestContext.getUniqueId(), ioException.getMessage());
		}
	}

	private ApiResponse<String> apiResponse(HttpStatus status, String message, List<ErrorData> errors) {
		ErrorResponse error = new ErrorResponse(status.toString(), message, errors);
		return ApiResponse.error(Status.ERROR, error);
	}

	public ApiResponse<String> handleError(Throwable e) {
		if (e instanceof DataValidationException) {
			var exception = (DataValidationException) e;
			sendExceptionEmail(requestContext, e.getMessage(), e.getCause(), exception.getErrors());
			this.updateRequest(requestContext, "Data Validation", e.getMessage(), exception.getErrors());
			return apiResponse(HttpStatus.PRECONDITION_FAILED, e.getMessage(), exception.getErrors());
		} else if (e instanceof MetadataValidationException) {
			var exception = (MetadataValidationException) e;
			sendExceptionEmail(requestContext, e.getMessage(), e.getCause(), exception.getErrors());
			this.updateRequest(requestContext, "Metadata Validation", e.getMessage(), exception.getErrors());
			return apiResponse(HttpStatus.BAD_REQUEST, e.getMessage(), exception.getErrors());
		} else if (e instanceof ValidationException) {
			var exception = (ValidationException) e;
			sendExceptionEmail(requestContext, e.getMessage(), e.getCause(), exception.getErrors());
			this.updateRequest(requestContext, "Validation", e.getMessage(), exception.getErrors());
			return apiResponse(HttpStatus.BAD_REQUEST, e.getMessage(), exception.getErrors());
		} else if (e instanceof ConfigurationException) {
			var exception = (ConfigurationException) e;
			sendExceptionEmail(requestContext, e.getMessage(), e.getCause(), exception.getErrors());
			this.updateRequest(requestContext, "Configuration", e.getMessage(), exception.getErrors());
			return apiResponse(HttpStatus.BAD_REQUEST, e.getMessage(), exception.getErrors());
		} else if (e instanceof TransformationException) {
			// var exception = (TransformationException) e;
			sendExceptionEmail(requestContext, e.getMessage(), e.getCause(), null);
			this.updateRequest(requestContext, "Transformation", e.getMessage(), null);
			return apiResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(), null);
		} else if (e instanceof GenerationException) {
			// var exception = (GenerationException) e;
			sendExceptionEmail(requestContext, e.getMessage(), e.getCause(), null);
			this.updateRequest(requestContext, "Generation", e.getMessage(), null);
			return apiResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(), null);
		} else if (e instanceof ReaderException) {
			// var exception = (ReaderException) e;
			sendExceptionEmail(requestContext, e.getMessage(), e.getCause(), null);
			this.updateRequest(requestContext, "Reader", e.getMessage(), null);
			return apiResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(), null);
		}

		sendExceptionEmail(requestContext, e.getMessage(), e.getCause(), null);
		this.updateRequest(requestContext, "Process", e.getMessage(), null);
		return apiResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(), null);
	}

	public void clean(String dir) {
		try {
			Files.walkFileTree(Paths.get(dir), new SimpleFileVisitor<Path>() {

				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					Files.delete(file);
					return FileVisitResult.CONTINUE;
				}

				@Override
				public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
					Files.delete(dir);
					return FileVisitResult.CONTINUE;
				}

			});
		} catch (IOException e) {
			log.error("AbstractFilesProcessorService -> clean() Failed to clean request files/folders, uniqueId = {}, errorDetails = {}", requestContext.getUniqueId(), e.getMessage());
		}
	}
	
	//Perform validations weather files are there or not in location - Not Using	
	public void validate(RequestPayload request) throws ValidationException {
		log.info(
				"SharedFilesProcessorService - process()  Started checking files from the given location. uniqueId = {}, appCode = {}",
				requestContext.getUniqueId(), appCode);
		// boolean isFound = request.getDocuments().parallelStream().allMatch(document -> sharedFilesService.isFileExists(document.getLocation(), appCode));
		if (request.getDocuments() == null || request.getDocuments().isEmpty()) {
			List<ErrorData> errors = new ArrayList<>();
			errors.add(new ErrorData("documents", "Documents can't be null or empty"));
			var validationException = new ValidationException("Request Validation - Documents can't be null or empty");
			validationException.setErrors(errors);
			validationException.setRequestContext(requestContext);
			throw validationException;
		}
		var errors = request.getDocuments().parallelStream().map(document -> {
			if (!iFilesService.isFileExists(document.getLocation(), appCode))
				return new ErrorData(document.getSourceKey(), document.getLocation());
			return null;
		}).filter(Objects::nonNull).collect(Collectors.toList());

		if (errors != null && !errors.isEmpty()) {
			var validationException = new ValidationException(
					"Request Validation - Files not present in Shared Location");
			validationException.setErrors(errors);
			validationException.setRequestContext(requestContext);
			throw validationException;
		}
	}
	
	// Perform Metadata Validation - Not Using
	@SuppressWarnings("unused")
	private void validate(List<Document> documents, RequestPayload request, Map<String, FileMetaData> fileMetaDataMap) {		
		
		//Pending Metadata validation
		var headerErrors = documents.parallelStream().map(document -> {
			MultipartFile sharedFile = document.getFile();
			// var fileName = fileUtils.getFileName(sharedFile.getOriginalFilename());
			// var fileExtension = fileUtils.getFileExtension(sharedFile.getOriginalFilename());
			var fileNameKey = document.getSourceKey();			
			var fileExtension = fileUtils.getFileExtension(document.getLocalPath());

			switch (fileExtension.toLowerCase()) {
			case "xlsx":
				// Metadata validation pass MultipartFile data object as input
				fileValidationUtils.validate(document.getLocalPath(), fileExtension.toLowerCase());
				//objectUtilsService.customValidatorDynamicAutowireService.validate(ExcelMetadataValidationService.class,	sharedFile, this.requestContext);
				try (XSSFWorkbook workBook = new XSSFWorkbook(sharedFile.getInputStream())) {
					int noOfSheets = workBook.getNumberOfSheets();
					var excelErrors = IntStream.range(0, noOfSheets).parallel().mapToObj(index -> {
						var sheet = workBook.getSheetAt(index);
						var key = (noOfSheets == 1) ? fileNameKey : fileNameKey.concat("{{").concat(sheet.getSheetName()).concat("}}");
						log.info("SharedFilesProcessorService -> process() uniqueId = {}, key = {}, headerIndexMap = {}", request.getUniqueId(), key, fileMetaDataMap);
						try {
							if (!columnRelationMap.containsKey(key))
								throw new MetadataValidationException("Request Validation - File Name in the input request is not matching with sourceKey present in the configuration");

							var dbHeaders = columnRelationMap.get(key).parallelStream().map(item -> item.getColumnName()).collect(Collectors.toList());

							validate(sheet, fileMetaDataMap.get(key).getHeaderIndex(), dbHeaders);
						} catch (MetadataValidationException e) {
							return new ErrorData(key, e.getMessage());
						}
						return null;
					}).filter(Objects::nonNull).collect(Collectors.toList());

					if (excelErrors != null && !excelErrors.isEmpty())
						log.error("SharedFilesProcessorService -> process() Failed validating excel headers, uniqueId = {}, errors = {}", request.getUniqueId(), excelErrors);
					return excelErrors;
				} catch (IOException e) {
					log.error("SharedFilesProcessorService -> process() Failed process excel file, uniqueId = {}, fileName = {}, error = {}", request.getUniqueId(), sharedFile.getOriginalFilename(), e.getMessage());
					return List.of(new ErrorData(sharedFile.getOriginalFilename(), e.getMessage()));
				}
			case "csv":
				fileValidationUtils.validate(document.getLocalPath(), fileExtension.toLowerCase());
				//objectUtilsService.customValidatorDynamicAutowireService.validate(CSVMetadataValidationService.class, sharedFile, this.requestContext);
				return null;

			case "json":
				return null;

			default:
				return null;

			}
		}).filter(Objects::nonNull).flatMap(List::stream).collect(Collectors.toList());

		if (headerErrors != null && !headerErrors.isEmpty()) {
			log.error("SharedFilesProcessorService -> process() Failed validating headers, uniqueId = {}, errors = {}",	request.getUniqueId(), headerErrors);
			var metadataValidationException = new MetadataValidationException("Headers Validation Failed");
			metadataValidationException.setErrors(headerErrors);
			metadataValidationException.setRequestContext(requestContext);
			throw metadataValidationException;
		}
	}

	//Data Vallidations like client and CAM
	public void validate(List<DataSet<DataMap>> dataSets, RequestPayload request, ValidationType validationType) throws IOException {
		var requestDir = largeRequestFilePath + requestContext.getRequestUuid();
		var dataSetDir = requestDir + "/datasets";
		var status = validationType.equals(ValidationType.client) ? Status.CLIENT_DATA_VALIDATION : Status.CAM_DATA_VALIDATION;
		
		HashMap<UUID, Boolean> dataSetValidationMap = new HashMap<>();
		
		dataSets.stream().forEach(temp -> {
			var currentDataSetDir = Paths.get(dataSetDir + "/" + temp.getId());
			var chunkDataSetDir = Paths.get(currentDataSetDir + "/chunks");
			var validationDir = Paths.get(currentDataSetDir + "/validation" + "/" + validationType.getValidationType());
			var batchSize = temp.getBatchSize();
			
			int[] count = { 0 };
			
			int[] batchNumber = { 0 };
			try (Stream<Path> paths = Files.list(chunkDataSetDir).filter(path -> path.toFile().isFile())) {		
				if (Files.notExists(validationDir)) {
					Files.createDirectories(validationDir);
				}
				Flux.fromStream(paths).map(path -> {
					try {
						batchNumber[0] += 1;
						var sReader = new FileReader(path.toFile());
						var dataSetRows = objectUtilsService.objectMapper.readValue(sReader, new TypeReference<List<DataMap>>() {});
					
						DataSet<DataMap> dataSet = new DataSet<>();
						dataSet.setId(temp.getId());
						dataSet.setName(temp.getName());
						dataSet.setData(dataSetRows);
						dataSet.setBatchSize(dataSetRows.size());
						dataSet.setBatchName("batch_" + batchNumber[0]);
						return this.validate(dataSet, validationType);
						
					} catch (IOException e) {
						throw new ProcessException(e.getMessage(), e);
					}
				})
				.window(windowBatchSize)	
				.concatMap(batch -> batch.flatMap(response -> response)
						.filter(response -> status.equals(response.getStatus()))
						.map(response -> response.getData())
						.flatMap(rows -> Flux.fromStream(rows.stream()))
						.buffer(batchSize)
						.doOnNext(rows -> {
							//log.error("SharedFilesProcessorService -> process() found validation rows = {} ", rows.size());
							dataSetValidationMap.computeIfAbsent(UUID.fromString(temp.getId()), k -> true);					
							count[0] += 1;
							var file = Paths.get(validationDir + "/" + validationType.getValidationType() + "_" + count[0] + ".json").toFile();
							write(file, rows);
						})
						.map(rows -> rows.size())
						.collectList())				
				.onErrorResume(e -> {
					var validationException = new ValidationException("Failed Validations",e);
					validationException.setRequestContext(requestContext);
					return Mono.error(validationException);				
				})
				.blockLast();
				//.subscribe(data -> log.info("SharedFilesProcessorService -> process() batch = {}", data.size()));
			} catch (IOException e) {
				throw new ProcessException(e.getMessage(), e);
			}
		});
		
		if(!dataSetValidationMap.isEmpty()) {
			constructErrorFile(request, validationType, dataSetValidationMap);
			send(request, "/validation/");
			throw new DataValidationException("Failed with data validations. Please refer exception file", validationType);
		}
		log.info("SharedFilesProcessorService -> process() Completed validating " + validationType.getValidationType() + " validations, uniqueId = {}",	request.getUniqueId());
		this.updateRequest(request, RequestStatus.Validate);
	}
	
	// Dataset Transformation
	public void transform(List<DataSet<DataMap>> dataSets, RequestPayload request, Map<String, DataSetRules> dataSetRulesMap, Map<String, Map<String, String>> dataSetDynamicClauseValuesMap) {
		
		var requestDir = largeRequestFilePath + requestContext.getRequestUuid();
		var dataSetDir = requestDir + "/datasets";
		
		dataSets.stream().forEach(temp -> {
			var currentDataSetDir = Paths.get(dataSetDir + "/" + temp.getId());
			var chunkDataSetDir = Paths.get(currentDataSetDir + "/chunks");
			var transformDir = Paths.get(currentDataSetDir + "/transform");
			var batchSize = temp.getBatchSize();
			
			int[] count = { 0 };
			
			int[] batchNumber = { 0 };
			try (Stream<Path> paths = Files.list(chunkDataSetDir).filter(path -> path.toFile().isFile())) {	
				if (Files.notExists(transformDir)) {
					Files.createDirectories(transformDir);
				}
				Flux.fromStream(paths).map(path -> {
					try {
						 batchNumber[0] += 1;
						 var sReader = new FileReader(path.toFile());
						 var dataSetRows = objectUtilsService.objectMapper.readValue(sReader, new TypeReference<List<DataMap>>() {});
						 DataSet<DataMap> dataSet = new DataSet<>();
						 dataSet.setId(temp.getId());
						 dataSet.setName(temp.getName());
						 dataSet.setData(dataSetRows);
						 dataSet.setBatchSize(dataSetRows.size());
						 dataSet.setBatchName("batch_" + batchNumber[0]);
						 return this.transform(dataSet, Objects.nonNull(dataSetRulesMap) ? dataSetRulesMap.get(temp.getId()) : null, Objects.nonNull(dataSetDynamicClauseValuesMap) ? dataSetDynamicClauseValuesMap.get(temp.getId()) : null);
					 } catch(IOException e) {
							throw new ProcessException(e.getMessage(), e);
					 }
				})
				.window(windowBatchSize)
				.concatMap(batch -> batch
						.flatMap(response -> response)
						.filter(response -> Status.SUCCESS.equals(response.getStatus()))
						.filter(response -> Objects.nonNull(response.getData()))
						.map(response -> response.getData())
						.flatMap(rows -> Flux.fromStream(rows.stream()))
						.buffer(batchSize)					
						.doOnNext(rows -> {											
							count[0] += 1;
							var dataSetFileName = temp.getName() + "_" + count[0];
							var file = Paths.get(transformDir + "/" + dataSetFileName + ".json").toFile();
							write(file, rows);					
						})
						.map(rows -> rows.size())
						.collectList())
				.onErrorResume(e -> {
					var transformationException = new TransformationException("Failed Transformations",e);
					transformationException.setRequestContext(requestContext);
					return Mono.error(transformationException);					
				})
				.blockLast();
			} catch (IOException e) {
				throw new ProcessException(e.getMessage(), e);
			}
		});

		this.updateRequest(request, RequestStatus.Transform);
	}

	private Map<String, DataSet<Void>> getDataSetMap(List<DataSet<DataMap>> dataSets) {
		return dataSets.parallelStream().map(dataSet -> {
			var temp = new DataSet<Void>();
			temp.setId(dataSet.getId());
			temp.setName(dataSet.getName());
			var targetFormatMap = objectUtilsService.dataStudioConfigurationService.findTargetDataFormatBy(
					requestContext.getBu(), requestContext.getPlatform(), requestContext.getDataCategory(),
					dataSet.getId());
			temp.setTargetFormatMap(targetFormatMap);
			return temp;
		}).collect(Collectors.toMap(DataSet::getId, Function.identity()));

	}

	public void generate(List<DataSet<DataMap>> dataSets, RequestPayload request, String outputRulesJson) {
		// Generate Files
		var dataSetsMap = getDataSetMap(dataSets);
		JSONArray outputFileRulesJsonArray = new JSONArray(outputRulesJson);

		IntStream.range(0, outputFileRulesJsonArray.length()).forEach(index -> {
			JSONObject outputFileRuleJson = outputFileRulesJsonArray.getJSONObject(index);

			var outputFileType = (String) outputFileRuleJson.get("outputFileType");
			switch (outputFileType.toLowerCase()) {
			case "txt":
				var dataJsonArray = outputFileRuleJson.getJSONArray("data");
				var txtDataMap = IntStream.range(0, dataJsonArray.length()).parallel().mapToObj(dataIndex -> {
					var dataRowJson = dataJsonArray.getJSONObject(dataIndex);
					var dataDatasetName = (String) dataRowJson.get("dataSetName");
					return dataSetsMap.get(dataDatasetName);
				}).collect(Collectors.toMap(DataSet::getId, Function.identity(), (first, second) -> first));
				objectUtilsService.customGeneratorDynamicAutowireService.generateLarge(LargeTextGeneratorService.class, outputFileRuleJson, txtDataMap, this.requestContext);
				break;
			case "xml":
				objectUtilsService.customGeneratorDynamicAutowireService.generateLarge(LargeStAXXMLGeneratorService.class, outputFileRuleJson, dataSetsMap, this.requestContext);
				break;
			case "xlsx":
				var sheetsJsonArray = outputFileRuleJson.getJSONArray("sheets");
				var xlsxSheetsDataMap = IntStream.range(0, sheetsJsonArray.length()).parallel().mapToObj(sheetIndex -> {
					var sheetJson = sheetsJsonArray.getJSONObject(sheetIndex);
					var sheetDatasetName = (String) sheetJson.get("dataSetName");
					return dataSetsMap.get(sheetDatasetName);
				}).collect(Collectors.toMap(DataSet::getId, Function.identity(), (first, second) -> first));				

				objectUtilsService.customGeneratorDynamicAutowireService.generateLarge(LargeExcelGeneratorService.class, outputFileRuleJson, xlsxSheetsDataMap, this.requestContext);
				break;
			case "csv":
				objectUtilsService.customGeneratorDynamicAutowireService.generateLarge(LargeCSVGeneratorService.class, outputFileRuleJson, dataSetsMap, this.requestContext);
				break;
			}
		});
		this.updateRequest(request, RequestStatus.File);

	}
	
	// Share Files
	public void send(RequestPayload request, String location) throws IOException {
		
		var requestDir = largeRequestFilePath + requestContext.getRequestUuid();
		var outputDir = Paths.get(requestDir + "/"+ location);
		try (Stream<Path> paths = Files.walk(outputDir)) {
			paths.parallel().filter(path -> path.toFile().isFile()).map(path -> {

				SharedFile sharedFile = new SharedFile();
				var currentAppcode = ValidationUtil.isHavingValue(configurationData.getAppCode())
						? configurationData.getAppCode()
						: appCode;
				sharedFile.setAppCode(currentAppcode);

				IFileService iFileService = null;
				var targetLocation = ValidationUtil.isHavingValue(configurationData.getTargetLocation()) ? configurationData.getTargetLocation() : TargetLocation.SharedDrive.getTargetLocation();

				switch (TargetLocation.valueOf(targetLocation)) {
				case SharePoint:
					iFileService = objectUtilsService.objectProviderSharedFilesService.getObject(requestContext, TargetLocation.SharePoint);
					sharedFile.setPath(requestContext.getSaveFileLocation());
					sharedFile.setLocation(TargetLocation.SharePoint);
					break;
				default:
					iFileService = objectUtilsService.objectProviderSharedFilesService.getObject(requestContext, TargetLocation.SharedDrive);
					var targetFolderPath = objectUtilsService.fileUtils.getTargetFolderPath(requestContext.getSaveFileLocation(), currentAppcode);
					sharedFile.setPath(targetFolderPath);
					sharedFile.setLocation(TargetLocation.SharedDrive);
					break;
				}

				try {
					sharedFile.setBytes(Files.readAllBytes(path));
					sharedFile.setName(path.getFileName().toString());
				} catch (IOException e) {
					throw new ProcessException("Failed to load file from temp dir", e);
				}
				return iFileService.uploadFile(sharedFile);

			}).filter(response -> response.getStatus().equalsIgnoreCase("Failed")).findAny().ifPresent(response -> {
				var error = new IOException("Failed to upload file, reason = " + response.getReason());
				throw new ProcessException("Failed to upload file", error);
			});
		}

		
	}
	
	public void sendEmail(String location) throws IOException{
		var requestDir = largeRequestFilePath + requestContext.getRequestUuid();
		var outputDir = Paths.get(requestDir + "/"+ location);
		try (Stream<Path> paths = Files.walk(outputDir)) {
			var files = paths.parallel().filter(path -> path.toFile().isFile()).map(path -> {
				SharedFile sharedFile = new SharedFile();
				var currentAppcode = ValidationUtil.isHavingValue(configurationData.getAppCode())
						? configurationData.getAppCode()
						: appCode;
				sharedFile.setAppCode(currentAppcode);
				sharedFile.setName(path.getFileName().toString());
				var targetLocation = ValidationUtil.isHavingValue(configurationData.getTargetLocation()) ? 
						configurationData.getTargetLocation() : TargetLocation.SharedDrive.getTargetLocation();
				switch (TargetLocation.valueOf(targetLocation)) {
				case SharePoint:					
					sharedFile.setPath(requestContext.getSaveFileLocation());
					sharedFile.setLocation(TargetLocation.SharePoint);
					break;
				default:
					var targetFolderPath = objectUtilsService.fileUtils.getTargetFolderPath(requestContext.getSaveFileLocation(), currentAppcode);
					sharedFile.setPath(targetFolderPath);
					sharedFile.setLocation(TargetLocation.SharedDrive);
					break;
				}
				return sharedFile;

			}).collect(Collectors.toList());
			sendEmail(files);
		}
	}

	private void constructErrorFile(RequestPayload request, ValidationType validationType, HashMap<UUID, Boolean> dataSetValidationMap) {
		var requestDir = largeRequestFilePath + requestContext.getRequestUuid();
		var dataSetDir = requestDir + "/datasets";
		try {			
			SXSSFWorkbook workbook = new SXSSFWorkbook(100);
			var fileName = getErrorFileName(validationType);
			var outputDir = Paths.get(requestDir + "/validation");
			var outputPath = Paths.get(outputDir + "/" + fileName);
			if (Files.notExists(outputDir)) {
				Files.createDirectories(outputDir);
			}
			FileOutputStream fileOut = new FileOutputStream(outputPath.toFile());
			var channel = FileChannel.open(outputPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);

			dataSetValidationMap.entrySet().forEach(entry -> {
				var currentDataSetDir = Paths.get(dataSetDir + "/" + entry.getKey().toString());
				var validationDir = Paths.get(currentDataSetDir + "/validation" + "/" + validationType.getValidationType());
				var file = validationDir.toFile();
				var filesCount = Objects.nonNull(file) && Objects.nonNull(file.listFiles()) ? file.listFiles().length : 0;
				if (filesCount > 0) {
					var sourceKeysRuleMap = new HashMap<String, LinkedHashMap<Integer, ColumnRelation>>();
					columnRelationMap.forEach((key, value) -> {
						var filterList = value.stream().filter(cr -> "Y".equalsIgnoreCase(cr.getColumnRequiredInErrorFile()))
								.collect(Collectors.toList());
						var rulesMap = IntStream.range(0, filterList.size()).boxed().collect(
								Collectors.toMap(Function.identity(), filterList::get, (e1, e2) -> e1, LinkedHashMap::new));
						sourceKeysRuleMap.put(key, rulesMap);
					});

					var sourceKeysColumnRelationsMap = new HashMap<String, Map<UUID, ColumnRelation>>();
					columnRelationMap.forEach((key, value) -> {
						var map = value.parallelStream()
								.collect(Collectors.toMap(cr -> UUID.fromString(cr.getUuid()), Function.identity()));
						sourceKeysColumnRelationsMap.put(key, map);
					});

					try (Stream<Path> paths = Files.list(validationDir)) {
						int[] count = { 0 };
						paths.forEach(path -> {
							try {
								if (count[0] == 0) {
									constructErrorHeaders(workbook, sourceKeysRuleMap);
								}
								count[0] += 1;
								var sReader = new FileReader(path.toFile());
								var errorRows = objectUtilsService.objectMapper.readValue(sReader, new TypeReference<List<Row>>() {});
								var sourceKeyErrorRowsMap = constructErrorData(errorRows);
								constructErrorRows(workbook, sourceKeysColumnRelationsMap, sourceKeysRuleMap, sourceKeyErrorRowsMap);
							} catch (IOException e) {
								throw new ProcessException(e.getMessage(), e);
							}

						});
					} catch (IOException e) {
						throw new ProcessException(e.getMessage(), e);
					}					
				}
			});
			workbook.write(fileOut);
			fileOut.flush();
			fileOut.close();
			workbook.close();
			channel.close();
			log.info("SharedFilesProcessorService -> constructErrorFile() Completed creating error file for "+ validationType.getValidationType() +" validation, uniqueId = {}", request.getUniqueId());			
		} catch(IOException e) {
			log.info("SharedFilesProcessorService -> constructErrorFile() Failed to create error file for "+ validationType.getValidationType() +" validation, uniqueId = {}", request.getUniqueId());	
			throw new ProcessException(e.getMessage(), e);
		}
	}
	
	private String getErrorFileName(ValidationType exceptionType) {
		return requestContext.getUniqueId()
				+ (ValidationType.CAM.equals(exceptionType) ? "_CAM_Exception_" : "_Exception_")
				+ requestContext.getDataCategory() + ".xlsx";
	}

	private void constructErrorHeaders(SXSSFWorkbook workbook,
			Map<String, LinkedHashMap<Integer, ColumnRelation>> sourceKeysRulesMap) throws IOException {
		sourceKeysRulesMap.entrySet().forEach(entry -> {
			var sheet = workbook.createSheet(entry.getKey());
			constructErrorHeaders(sheet, entry.getValue());
		});
	}

	private void constructErrorHeaders(SXSSFSheet sheet, LinkedHashMap<Integer, ColumnRelation> rulesMap) {
		org.apache.poi.ss.usermodel.Row rowHeader = sheet.createRow(0);

		if (!rulesMap.isEmpty()) {
			IntStream.range(0, rulesMap.size()).forEach(headerIndex -> {
				var columnName = rulesMap.get(headerIndex).getColumnName();
				rowHeader.createCell(headerIndex).setCellValue(columnName);
			});
		}
		var headerIndex = rulesMap.size();
		rowHeader.createCell(headerIndex).setCellValue("Field with error");
		rowHeader.createCell(headerIndex + 1).setCellValue("Field Value");
		rowHeader.createCell(headerIndex + 2).setCellValue("Error Message");
	}

	private void constructErrorRows(SXSSFWorkbook workbook,
			Map<String, Map<UUID, ColumnRelation>> sourceKeysColumnRelationsMap,
			Map<String, LinkedHashMap<Integer, ColumnRelation>> sourceKeysRulesMap,
			Map<String, List<Row>> sourceKeyRowsMap) {
		sourceKeysRulesMap.entrySet().forEach(entry -> {
			var rows = sourceKeyRowsMap.get(entry.getKey());
			if(Objects.nonNull(rows) && !rows.isEmpty())
				constructErrorRows(workbook.getSheet(entry.getKey()), sourceKeysColumnRelationsMap.get(entry.getKey()),	entry.getValue(), rows);
		});
	}

	private void constructErrorRows(SXSSFSheet sheet, Map<UUID, ColumnRelation> sourceKeyColumnRelationsMap,
			LinkedHashMap<Integer, ColumnRelation> rulesMap, List<Row> rows) {
		var dataRowIndex = sheet.getLastRowNum() + 1;
		var headerIndex = rulesMap.size();
		for (Row row : rows) {
			var columns = row.getColumns().values();
			for (Column column : columns) {
				if (Objects.nonNull(column.getErrors()) && !column.getErrors().isEmpty()) {
					var errors = column.getErrors();
					for (String errorMessage : errors) {
						org.apache.poi.ss.usermodel.Row excelRow = sheet.createRow(dataRowIndex);
						if (!rulesMap.isEmpty()) {
							IntStream.range(0, rulesMap.size()).forEach(columnIndex -> {
								var columnUuid = UUID.fromString(rulesMap.get(columnIndex).getUuid());
								var tempColumn = row.getColumns().get(columnUuid);
								excelRow.createCell(columnIndex).setCellValue(
										(Objects.nonNull(tempColumn) && Objects.nonNull(tempColumn.getValue()))
												? tempColumn.getValue().toString()
												: "");
							});
						}
						excelRow.createCell(headerIndex)
								.setCellValue(sourceKeyColumnRelationsMap.get(column.getUuid()).getColumnName());
						excelRow.createCell(headerIndex + 1).setCellValue(
								ValidationUtil.isHavingValue(column.getSourceValue()) ? column.getSourceValue() : null);
						excelRow.createCell(headerIndex + 2).setCellValue(errorMessage);
						dataRowIndex++;
					}
				}
			}
		}
	}

	
	@SuppressWarnings("unused")
	private void constructErrorFile(Map<String, List<Row>> sourceKeyMap, String exceptionType, String mode)
			throws IOException {

		var fileName = requestContext.getUniqueId()
				+ ("CAM".equalsIgnoreCase(exceptionType) ? "_CAM_Exception_" : "_Exception_")
				+ requestContext.getDataCategory() + ".xlsx";
		var outputDir = Paths.get(largeRequestFilePath + requestContext.getRequestUuid() + "/validation");
		var outputPath = Paths.get(outputDir + "/" + fileName);
		if (Files.notExists(outputDir)) {
			Files.createDirectories(outputDir);
		}

		FileOutputStream fileOut = new FileOutputStream(outputPath.toFile());
		var channel = FileChannel.open(outputPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
		SXSSFWorkbook workbook = "CREATE".equalsIgnoreCase(mode) ? new SXSSFWorkbook(100)
				: ((Supplier<SXSSFWorkbook>) () -> {
					var file = outputPath.toFile();
					XSSFWorkbook wb;
					try {
						FileInputStream fileInputStream = new FileInputStream(file);
						wb = new XSSFWorkbook(fileInputStream);
						return new SXSSFWorkbook(wb, 100);
					} catch (IOException e) {
						throw new ProcessException(e.getMessage(), e);
					}

				}).get();

		if ("CREATE".equalsIgnoreCase(mode)) {
			sourceKeyMap.entrySet().forEach(entity -> {
				var sourceKeyColumnRelations = columnRelationMap.get(entity.getKey());
				var filteredRules = sourceKeyColumnRelations.parallelStream()
						.filter(rule -> "Y".equalsIgnoreCase(rule.getColumnRequiredInErrorFile()))
						.collect(Collectors.toList());

				var rulesMap = IntStream.range(0, filteredRules.size()).boxed().collect(Collectors
						.toMap(Function.identity(), sourceKeyColumnRelations::get, (e1, e2) -> e1, LinkedHashMap::new));

				SXSSFSheet sheet = workbook.createSheet(entity.getKey());
				org.apache.poi.ss.usermodel.Row rowHeader = sheet.createRow(0);

				if (!rulesMap.isEmpty()) {
					IntStream.range(0, rulesMap.size()).forEach(headerIndex -> {
						var columnName = rulesMap.get(headerIndex).getColumnName();
						rowHeader.createCell(headerIndex).setCellValue(columnName);
					});
				}
				var headerIndex = rulesMap.size();
				rowHeader.createCell(headerIndex).setCellValue("Field with error");
				rowHeader.createCell(headerIndex + 1).setCellValue("Field Value");
				rowHeader.createCell(headerIndex + 2).setCellValue("Error Message");
			});
		}

		sourceKeyMap.entrySet().forEach(entity -> {
			var sourceKeyColumnRelations = columnRelationMap.get(entity.getKey());
			var sourceKeyColumnRelationsMap = sourceKeyColumnRelations.parallelStream()
					.collect(Collectors.toMap(cr -> UUID.fromString(cr.getUuid()), Function.identity()));
			var rules = sourceKeyColumnRelations.stream()
					.filter(item -> "Y".equalsIgnoreCase(item.getColumnRequiredInErrorFile()))
					.collect(Collectors.toList());

			LinkedHashMap<Integer, ColumnRelation> rulesMap = new LinkedHashMap<>();

			int i = 0;
			while (i < rules.size()) {
				rulesMap.put(i, rules.get(i));
				i++;
			}

			SXSSFSheet sheet = workbook.createSheet(entity.getKey());
			org.apache.poi.ss.usermodel.Row rowHeader = sheet.createRow(0);

			if (!rulesMap.isEmpty()) {
				IntStream.range(0, rulesMap.size()).forEach(headerIndex -> {
					var columnName = rulesMap.get(headerIndex).getColumnName();
					rowHeader.createCell(headerIndex).setCellValue(columnName);
				});
			}
			var headerIndex = rulesMap.size();
			rowHeader.createCell(headerIndex).setCellValue("Field with error");
			rowHeader.createCell(headerIndex + 1).setCellValue("Field Value");
			rowHeader.createCell(headerIndex + 2).setCellValue("Error Message");

			var rows = entity.getValue();
			// var dataRowIndex = 1;
			var dataRowIndex = sheet.getLastRowNum();
			for (Row row : rows) {
				var columns = row.getColumns().values();
				for (Column column : columns) {
					if (Objects.nonNull(column.getErrors()) && !column.getErrors().isEmpty()) {
						var errors = column.getErrors();
						for (String errorMessage : errors) {
							org.apache.poi.ss.usermodel.Row excelRow = sheet.createRow(dataRowIndex);
							if (!rulesMap.isEmpty()) {
								IntStream.range(0, rulesMap.size()).forEach(columnIndex -> {
									var columnUuid = UUID.fromString(rulesMap.get(columnIndex).getUuid());
									var tempColumn = row.getColumns().get(columnUuid);
									excelRow.createCell(columnIndex).setCellValue(
											(Objects.nonNull(tempColumn) && Objects.nonNull(tempColumn.getValue()))
													? tempColumn.getValue().toString()
													: "");
								});
							}
							excelRow.createCell(headerIndex)
									.setCellValue(sourceKeyColumnRelationsMap.get(column.getUuid()).getColumnName());
							excelRow.createCell(headerIndex + 1)
									.setCellValue(ValidationUtil.isHavingValue(column.getSourceValue())
											? column.getSourceValue()
											: null);
							excelRow.createCell(headerIndex + 2).setCellValue(errorMessage);
							dataRowIndex++;
						}
					}
				}
			}

		});

		workbook.write(fileOut);
		fileOut.flush();
		fileOut.close();
		workbook.close();
		channel.close();

	}

	public void write(File file, Object value) {
		CompletableFuture.runAsync(() -> {
			try {
				objectUtilsService.objectMapper.writeValue(file, value);
			} catch (IOException e) {
				throw new ProcessException(e.getMessage(), e);
			}
		});
	}
	
	public List<DynamicClause> constructClause(List<DynamicClause> dynamicClauses, List<DataMap> rows){
		
		Function<Double, String> doubleToStringFunction = value -> {
			if(Objects.nonNull(value)) {
				var result = Double.toString(value);
				return result.contains(".") ? result.replaceAll("0*$", "").replaceAll("\\.$", "") : result;					
			}
			return null;
		};
		
		
		BiFunction<DataMap, String, Object> sourceDataFunction = (row,uuid) -> row.getColumns().get(UUID.fromString(uuid));
		BiFunction<List<DataMap>, String ,DoubleStream> rowsFilterFunction = (rowsData, column) -> rowsData.stream().map(row -> sourceDataFunction.apply(row, column)).filter(data -> Objects.nonNull(data)).mapToDouble(data -> Double.valueOf(data.toString()));
		
		return dynamicClauses.stream().map(dynamicClause -> {
			var column = dynamicClause.getColumn();
			if(dynamicClause.getOperator().equalsIgnoreCase("Max")) {
				var optionalCurrent = rowsFilterFunction.apply(rows, column).max();
				var current = optionalCurrent.isPresent()? optionalCurrent.getAsDouble() : 0.0;
				if(ValidationUtil.isHavingValue(dynamicClause.getValue())) {
					var existing = Double.parseDouble(dynamicClause.getValue());
					var max = Math.max(existing, current);
					dynamicClause.setValue(doubleToStringFunction.apply(max));
				} else {
					dynamicClause.setValue(doubleToStringFunction.apply(current));
				}
			} else if(dynamicClause.getOperator().equalsIgnoreCase("Min")) {
				var optionalCurrent = rowsFilterFunction.apply(rows, column).min();
				var current = optionalCurrent.isPresent()? optionalCurrent.getAsDouble() : 0.0;
				if(ValidationUtil.isHavingValue(dynamicClause.getValue())) {
					var existing = Double.parseDouble(dynamicClause.getValue());
					var min = Math.min(existing, current);
					dynamicClause.setValue(doubleToStringFunction.apply(min));
				} else {
					dynamicClause.setValue(doubleToStringFunction.apply(current));
				}
			}
			
			return dynamicClause;
		}).toList();
		
		
	}
	
	public Map<String, List<DynamicClause>> getClause(Map<String, DataSetRules> dataSetRulesMap) {
		return dataSetRulesMap.entrySet().parallelStream()
				.filter(entry -> {
					var dataSetRule = entry.getValue();
					if(Objects.nonNull(dataSetRule.getGroupBy()) 
							&& Objects.nonNull(dataSetRule.getGroupBy().getClause())
							&& !dataSetRule.getGroupBy().getClause().isEmpty()){				
						return true;
					}
					return false;
				})
				.collect(Collectors.toMap(Entry::getKey, entry -> {
					var dataSetRule = entry.getValue();
					return dataSetRule.getGroupBy().getClause()
							.stream()
							.filter(dynamicClause -> dynamicClause.getLevel().equalsIgnoreCase("Global"))
							.collect(Collectors.toList());
				}));
	}
}
