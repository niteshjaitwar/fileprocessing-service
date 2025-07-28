package com.adp.esi.digitech.file.processing.reader.service;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.csv.CSVParser;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.file.processing.ds.config.model.FileMetaData;
import com.adp.esi.digitech.file.processing.ds.model.ColumnRelation;
import com.adp.esi.digitech.file.processing.exception.ReaderException;
import com.adp.esi.digitech.file.processing.file.service.IFileService;
import com.adp.esi.digitech.file.processing.model.ChunkDataMap;
import com.adp.esi.digitech.file.processing.model.DataMap;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

@Service("largeCSVReaderService")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class LargeCSVReaderService extends AbstractCSVReaderService<ChunkDataMap, String> {
	
	@Autowired
	ObjectProvider<IFileService> fileService;
	
	@Autowired
	ObjectMapper objectMapper;
	
	@Autowired
	Executor asyncExecutor;
	
	//@Value("${large.request.file.path}")
	//String largeRequestFilePath;
	
	private List<ColumnRelation> columnRelations;
	private FileMetaData fileMetaData;	
	
	@Autowired(required = true)
	public LargeCSVReaderService(FileMetaData fileMetaData, List<ColumnRelation> columnRelations) {
		this.fileMetaData = fileMetaData;
		this.columnRelations = columnRelations;
	}
	
	@Override
	public Map<String, List<ChunkDataMap>> read(String filePath) throws ReaderException {
		log.info("LargeCSVReaderService -> read() Started processing csv, uniqueId = {}, sourceKey = {}", requestContext.getUniqueId(), fileMetaData.getSourceKey());
		
		//CSVFormat format = CSVFormat.Builder.create(CSVFormat.EXCEL).setDelimiter(fileMetaData.getDelimeter()).build();
		Map<String, List<String>> metaData = new HashMap<>();
		int[] masterCount = {0};
		try(var reader = Files.newBufferedReader(Paths.get(filePath), StandardCharsets.ISO_8859_1)) {
			
			var format = this.newCsvFormat(fileMetaData, reader);
			@Cleanup var parser = CSVParser.parse(reader, format);
			
			if(HEADER.equalsIgnoreCase(fileMetaData.getProcessing())) {
				this.validate(fileMetaData, columnRelations, parser.getHeaderNames());
			}
			
			
			var dir = Paths.get(largeRequestFilePath + requestContext.getRequestUuid() + "/" + fileMetaData.getSourceKey());
        	
        	if(Files.notExists(dir)) {
        		Files.createDirectories(dir);
        	}			
			
			List<DataMap> rows = new ArrayList<>();
			AtomicInteger fileCounter = new AtomicInteger();
			parser.stream().forEach(record -> {
				
				if(!HEADER.equalsIgnoreCase(fileMetaData.getProcessing()) && record.getRecordNumber() == 1) {
					this.validate(fileMetaData, columnRelations, record);
				}
				var columnMap = columnRelations.parallelStream()
						.collect(HashMap<UUID,String>::new, 
						(map,columnRelation) -> map.put(UUID.fromString(columnRelation.getUuid()), getValue(record, columnRelation, fileMetaData.getProcessing())), 
						HashMap<UUID,String>::putAll);	
				
				DataMap dataRow = new DataMap(columnMap);
				rows.add(dataRow);				
				
				if(rows.size() >= fileMetaData.getBatchSize()) {
					masterCount[0] += rows.size();					
					
					var batchData = new ArrayList<>(rows);				
					var fileName = fileMetaData.getSourceKey()+ "_" + fileCounter.incrementAndGet();
					var groupRows = batchData.parallelStream().collect(Collectors.groupingBy(row -> row.getColumns().get(UUID.fromString(fileMetaData.getGroupIdentifier()))));
					var keys = groupRows.keySet().stream().collect(Collectors.toList());					
					
					CompletableFuture.runAsync(() -> {						
						write(dir, fileName, groupRows);
					}, asyncExecutor);
					
					metaData.put(fileName, keys);
					rows.clear();
				}
			});
			
			if(!rows.isEmpty()) {
				masterCount[0] += rows.size();
				var fileName = fileMetaData.getSourceKey()+ "_" + fileCounter.incrementAndGet();
				var batchData = new ArrayList<>(rows);
				var groupRows = batchData.parallelStream().collect(Collectors.groupingBy(row -> row.getColumns().get(UUID.fromString(fileMetaData.getGroupIdentifier()))));
				var keys = groupRows.keySet().stream().collect(Collectors.toList());		
									
				write(dir, fileName, groupRows);
				
				metaData.put(fileName, keys);
				rows.clear();
			}
			
			CompletableFuture.runAsync(() -> {
				write(dir, "meta", metaData);
			}, asyncExecutor);
			
			Map<String, List<String>> sourceDataMap = new HashMap<>();
			metaData.forEach((key, value) -> value.forEach(item -> sourceDataMap.computeIfAbsent(item, k -> new ArrayList<>()).add(key)));
			
			var chunks= sourceDataMap.entrySet().stream().map(entry -> new ChunkDataMap(entry.getKey(), entry.getValue())).collect(Collectors.toList());
			
			var data = new HashMap<String, List<ChunkDataMap>>();	
			data.put(fileMetaData.getSourceKey(), chunks);
			log.info("LargeCSVReaderService -> read() Completed processing csv, uniqueId = {}, sourceKey = {}, Total Records = {}", requestContext.getUniqueId(), fileMetaData.getSourceKey(), masterCount[0]);
			return data;
         } catch (IOException e) {
        	 throw new ReaderException(e.getMessage(), e);
         }
	}
	
	@SuppressWarnings("unused")
	private <T> void write1(Path dir, String fileName,  Map<String,List<T>> data) throws ReaderException {	
		var file = Paths.get(dir + "/" + fileName + ".json.gz").toFile();
        try(FileOutputStream fos = new FileOutputStream(file);
        	GZIPOutputStream gos = new GZIPOutputStream(fos);
        	OutputStreamWriter osw = new OutputStreamWriter(gos, StandardCharsets.ISO_8859_1);
        	BufferedWriter writer = new BufferedWriter(osw)) {
        	var dataStr = objectMapper.writeValueAsString(data);
        	writer.write(dataStr);
			
		} catch (IOException e) {
			throw new ReaderException(e.getMessage(), e);
		}
    }
}
