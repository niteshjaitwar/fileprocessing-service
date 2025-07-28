package com.adp.esi.digitech.file.processing.validation.service;

import java.util.LinkedList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.file.processing.exception.MetadataValidationException;

import lombok.extern.slf4j.Slf4j;

@Service("jsonMetadataValidatorService")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class JSONMetadataValidationService extends AbstractValidationService<JSONObject> {

	@Override
	public void validate(JSONObject data) throws MetadataValidationException {	
		log.info("JSONMetadataValidatorService -> validate() Started JSON metadata validations, uniqueId = {}", requestContext.getUniqueId());		
		String message = validateMetadata(data);
		if(message != null && !message.isBlank() && !message.isEmpty()) {
			log.error("JSONMetadataValidatorService -> validate() Failed metadata validations, uniqueId = {}, Error Message = {}", requestContext.getUniqueId(), message);	
			var metadataValidationException = new MetadataValidationException(message);
			metadataValidationException.setRequestContext(requestContext);
			throw metadataValidationException;
		}
		log.info("JSONMetadataValidatorService -> validate() Completed JSON metadata validations, uniqueId = {}", requestContext.getUniqueId());	
	}
	
	private String validateMetadata(JSONObject payload) {
		String message = "";
		for(String item : payload.keySet()){
			switch (item.toUpperCase()) {
			case "TEXT":
				JSONObject textJson = (JSONObject) payload.get(item);
				
				JSONArray textHeaders = (JSONArray) textJson.get("headers");		
				if(textHeaders == null || textHeaders.isEmpty())
					message = message.concat("The request " + requestContext.getUniqueId() + " TEXT -> Section has missing headers \n");
				
				JSONArray textDataArray = (JSONArray)textJson.get("data");
				if(textDataArray == null || textDataArray.isEmpty())
					message = message.concat("The request "+ requestContext.getUniqueId() +" TEXT -> Section has missing data \n");				
				
				break;
				
			case "TABS":
				if(payload.get(item) instanceof JSONArray) {
					JSONArray tabs = (JSONArray) payload.get(item);
					List<String> tabList = new LinkedList<>();
					for (Object tab : tabs) {
						JSONObject jTab = (JSONObject) tab;							
						
						String tabsValidation = validateMetadata(jTab);
						if(tabsValidation != null && !tabsValidation.isBlank() && !tabsValidation.isEmpty()) 
							tabList.add(tabsValidation);						
					}
					if(tabList != null && tabList.size() > 0 && !tabList.isEmpty()) {
						message = message.concat(item + "[\n");
						for (String data : tabList) {
							message = message.concat("\t" + data + "\n");
						}
						
						message = message.concat("]\n");
					}
				}	
				break;

			case "DATA_GRID":
				
				JSONObject dataGridJson = (JSONObject)payload.get(item);
				JSONArray dataGridHeaders = (JSONArray) dataGridJson.get("headers");				
				if(dataGridHeaders == null || dataGridHeaders.isEmpty())
					message = message.concat("The request "+ requestContext.getUniqueId() +" DATA_GRID -> Section has missing headers \n");
				
				JSONArray dataGridDataArray = (JSONArray)dataGridJson.get("data");
				if(dataGridDataArray == null || dataGridDataArray.isEmpty())
					message = message.concat("The request "+ requestContext.getUniqueId() +" DATA_GRID -> Section has missing data \n");			
			
				break;

			default:
				if(item.toUpperCase().matches("^[T]{1}[A]{1}[B]{1}[_]{1}\\d{1,}$")) {
					JSONArray tab = (JSONArray) payload.get(item);
					
					for (Object section : tab) {
						JSONObject jSection = (JSONObject) section;
						String tabValidations = validateMetadata(jSection);
						if(tabValidations != null && !tabValidations.isBlank() && !tabValidations.isEmpty()) {
							message = message.concat(item + "[\n");
							message = message.concat("\t\t"+tabValidations);						
							message = message.concat("\t]");
						}
					}
					
				}
				break;
			}
		}
		return message;
	}

}
