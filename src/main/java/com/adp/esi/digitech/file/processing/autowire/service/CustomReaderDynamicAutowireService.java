package com.adp.esi.digitech.file.processing.autowire.service;

import java.io.InputStream;
import java.io.LineNumberReader;
import java.util.List;
import java.util.Map;

import org.apache.poi.xssf.model.SharedStrings;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.multipart.MultipartFile;

import com.adp.esi.digitech.file.processing.ds.config.model.FileMetaData;
import com.adp.esi.digitech.file.processing.ds.model.ColumnRelation;
import com.adp.esi.digitech.file.processing.exception.ReaderException;
import com.adp.esi.digitech.file.processing.model.ChunkDataMap;
import com.adp.esi.digitech.file.processing.model.DataMap;
import com.adp.esi.digitech.file.processing.model.RequestContext;
import com.adp.esi.digitech.file.processing.reader.service.CSVReaderService;
import com.adp.esi.digitech.file.processing.reader.service.ExcelReaderService;
import com.adp.esi.digitech.file.processing.reader.service.FormReaderService;
import com.adp.esi.digitech.file.processing.reader.service.IReaderService;
import com.adp.esi.digitech.file.processing.reader.service.JsonHeaderReaderService;
import com.adp.esi.digitech.file.processing.reader.service.JsonReaderService;
import com.adp.esi.digitech.file.processing.reader.service.LargeCSVReaderService;
import com.adp.esi.digitech.file.processing.reader.service.LargeTextReaderService;
import com.adp.esi.digitech.file.processing.reader.service.OPCExcelReaderService;
import com.adp.esi.digitech.file.processing.reader.service.OPCSheetReaderService;
import com.adp.esi.digitech.file.processing.reader.service.SheetReaderService;
import com.adp.esi.digitech.file.processing.reader.service.TextReaderService;
import com.adp.esi.digitech.file.processing.reader.xml.service.LargeXMLReaderService;
import com.adp.esi.digitech.file.processing.reader.xml.service.XMLReaderService;

@Service("customReaderDynamicAutowireService")
public class CustomReaderDynamicAutowireService {
	
	private final WebApplicationContext webApplicationContext;
	
	@Autowired
	public CustomReaderDynamicAutowireService(WebApplicationContext webApplicationContext) {
		this.webApplicationContext = webApplicationContext;
	}
	
	public Map<String, List<DataMap>> readWorkbookData(XSSFWorkbook workbook, String fileName, Map<String, List<ColumnRelation>> columnRelationsMap, Map<String, FileMetaData> fileMetaDataMap, RequestContext requestContext) throws ReaderException {
		IReaderService<DataMap, XSSFWorkbook> readerService = webApplicationContext.getBean(ExcelReaderService.class, fileName, columnRelationsMap, fileMetaDataMap);
		readerService.setRequestContext(requestContext);
		return readerService.read(workbook);	
	}
	
	public Map<String, List<ChunkDataMap>> readLargeWorkbookData(String filePath, String fileName, Map<String, List<ColumnRelation>> columnRelationsMap, Map<String, FileMetaData> fileMetaDataMap, RequestContext requestContext) throws ReaderException {
		IReaderService<ChunkDataMap, String> readerService = webApplicationContext.getBean(OPCExcelReaderService.class, fileName, columnRelationsMap, fileMetaDataMap);
		readerService.setRequestContext(requestContext);
		return readerService.read(filePath);	
	}	
	
	public Map<String, List<DataMap>> readSheetData(XSSFSheet sheet, FileMetaData fileMetaData, List<ColumnRelation> columnRelations, RequestContext requestContext) throws ReaderException {
		IReaderService<DataMap, XSSFSheet> readerService = webApplicationContext.getBean(SheetReaderService.class, fileMetaData, columnRelations);
		readerService.setRequestContext(requestContext);
		return readerService.read(sheet);	
	}
	
	public Map<String, List<ChunkDataMap>> readLargeSheetData(InputStream sheet, FileMetaData fileMetaData,List<ColumnRelation> columnRelations, RequestContext requestContext, SharedStrings sst) throws ReaderException {
		IReaderService<ChunkDataMap, InputStream> readerService = webApplicationContext.getBean(OPCSheetReaderService.class, fileMetaData, columnRelations, sst);
		readerService.setRequestContext(requestContext);
		return readerService.read(sheet);	
	}	
	
	public Map<String, List<DataMap>> readJsonData(JSONObject rawPayload, Map<String, List<ColumnRelation>> columnRelationsMap, RequestContext requestContext) throws ReaderException {
		IReaderService<DataMap, JSONObject> readerService = webApplicationContext.getBean(JsonReaderService.class, columnRelationsMap);
		readerService.setRequestContext(requestContext);
		return readerService.read(rawPayload);	
	}
	
	public Map<String, List<String>> readJsonHeaders(JSONObject rawPayload, RequestContext requestContext) throws ReaderException {
		IReaderService<String, JSONObject> readerService = webApplicationContext.getBean(JsonHeaderReaderService.class);
		readerService.setRequestContext(requestContext);
		return readerService.read(rawPayload);	
	}
	
	public Map<String, List<DataMap>> readCSVData(MultipartFile file, FileMetaData fileMetaData, List<ColumnRelation> columnRelations, RequestContext requestContext) throws ReaderException {
		IReaderService<DataMap, MultipartFile> readerService = webApplicationContext.getBean(CSVReaderService.class, fileMetaData, columnRelations);
		readerService.setRequestContext(requestContext);
		return readerService.read(file);	
	}
	
	public Map<String, List<ChunkDataMap>> readLargeCSVData(String filePath, FileMetaData fileMetaData, List<ColumnRelation> columnRelations, RequestContext requestContext) throws ReaderException {
		IReaderService<ChunkDataMap, String> readerService = webApplicationContext.getBean(LargeCSVReaderService.class, fileMetaData, columnRelations);
		readerService.setRequestContext(requestContext);
		return readerService.read(filePath);	
	}
	
	public Map<String, List<DataMap>> readTXTData(MultipartFile file, FileMetaData fileMetaData, List<ColumnRelation> columnRelations, RequestContext requestContext) throws ReaderException {
		IReaderService<DataMap, MultipartFile> readerService = webApplicationContext.getBean(TextReaderService.class, fileMetaData, columnRelations);
		readerService.setRequestContext(requestContext);
		return readerService.read(file);	
	}
	
	public <T extends IReaderService<DataMap, LineNumberReader>> Map<String, List<DataMap>> readTXTData(Class<T> type, LineNumberReader reader, FileMetaData fileMetaData, Map<String, Map<String, ColumnRelation>> sourceLineColumnRelationMap, RequestContext requestContext) throws ReaderException {
		IReaderService<DataMap, LineNumberReader> readerService = webApplicationContext.getBean(type, fileMetaData, sourceLineColumnRelationMap);
		readerService.setRequestContext(requestContext);
		return readerService.read(reader);	
	}
	
	public Map<String, List<ChunkDataMap>> readLargeTXTData(String filePath, FileMetaData fileMetaData, List<ColumnRelation> columnRelations, RequestContext requestContext) throws ReaderException {
		IReaderService<ChunkDataMap, String> readerService = webApplicationContext.getBean(LargeTextReaderService.class, fileMetaData, columnRelations);
		readerService.setRequestContext(requestContext);
		return readerService.read(filePath);	
	}
	
	public <T extends IReaderService<ChunkDataMap, LineNumberReader>> Map<String, List<ChunkDataMap>> readLargeTXTData(Class<T> type, LineNumberReader reader, FileMetaData fileMetaData, Map<String, Map<String, ColumnRelation>> sourceLineColumnRelationMap, RequestContext requestContext) throws ReaderException {
		IReaderService<ChunkDataMap, LineNumberReader> readerService = webApplicationContext.getBean(type, fileMetaData, sourceLineColumnRelationMap);
		readerService.setRequestContext(requestContext);
		return readerService.read(reader);	
	}
	
	public Map<String, List<DataMap>> readFormData(JSONObject rawPayload, FileMetaData fileMetaData, List<ColumnRelation> columnRelations, RequestContext requestContext) throws ReaderException {
		IReaderService<DataMap, JSONObject> readerService = webApplicationContext.getBean(FormReaderService.class, fileMetaData, columnRelations);
		readerService.setRequestContext(requestContext);
		return readerService.read(rawPayload);	
	}
	
	public Map<String, List<DataMap>> readXMLData(MultipartFile file, FileMetaData fileMetaData, List<ColumnRelation> columnRelations, RequestContext requestContext) throws ReaderException {
		IReaderService<DataMap, MultipartFile> readerService = webApplicationContext.getBean(XMLReaderService.class, fileMetaData, columnRelations);
		readerService.setRequestContext(requestContext);
		return readerService.read(file);
	}
	
	public Map<String, List<ChunkDataMap>> readLargeXMLData(String filePath, FileMetaData fileMetaData, List<ColumnRelation> columnRelations, RequestContext requestContext) throws ReaderException {
		IReaderService<ChunkDataMap, String> readerService = webApplicationContext.getBean(LargeXMLReaderService.class, fileMetaData, columnRelations);
		readerService.setRequestContext(requestContext);
		return readerService.read(filePath);	
	}
}
