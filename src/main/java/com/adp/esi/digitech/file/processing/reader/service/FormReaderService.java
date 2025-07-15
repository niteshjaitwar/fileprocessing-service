package com.adp.esi.digitech.file.processing.reader.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.file.processing.ds.config.model.FileMetaData;
import com.adp.esi.digitech.file.processing.ds.model.ColumnRelation;
import com.adp.esi.digitech.file.processing.exception.ReaderException;
import com.adp.esi.digitech.file.processing.model.DataMap;

import lombok.extern.slf4j.Slf4j;


@Service("formReaderService")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class FormReaderService extends AbstractReaderService<DataMap,JSONObject> {
	
	private FileMetaData fileMetaData;
	private List<ColumnRelation> columnRelations;	
	
	@Autowired(required = true)
	public FormReaderService(FileMetaData fileMetaData, List<ColumnRelation> columnRelations) {		
		this.fileMetaData = fileMetaData;
		this.columnRelations = columnRelations;		
	}

	@Override
	public Map<String, List<DataMap>> read(JSONObject payload) throws ReaderException {
		try {
			var columnMap = columnRelations.stream().collect(HashMap<UUID,String>::new, 
					(map,columnRelation) -> map.put(UUID.fromString(columnRelation.getUuid()), getValue(payload, columnRelation.getColumnName()).get()), 
					HashMap<UUID,String>::putAll);		
					
					
			var data = new HashMap<String, List<DataMap>>();
			data.put(fileMetaData.getSourceKey(), List.of(new DataMap(columnMap)));
			return data;
		} catch (Exception e) {
			log.error("FormReaderService -> read() Failed to processing JSONObject, uniqueId = {}, message = {}", requestContext.getUniqueId(), e.getMessage());
			var readerException = new ReaderException("Form Parsing failed, reason = " + e.getMessage(), e.getCause());
			readerException.setRequestContext(requestContext);
			throw readerException;
		}
	}
	
	public Supplier<String> getValue(JSONObject payload, String columnName) {
		return () -> {
			return (payload.has(columnName) && !payload.isNull(columnName)) ? payload.getString(columnName) : null;
		};
	}
}
