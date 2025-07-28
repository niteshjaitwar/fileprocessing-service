package com.adp.esi.digitech.file.processing.generator.service;

import java.io.StringWriter;
import java.util.List;
import java.util.Map;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.TransformerException;

import org.apache.commons.collections4.map.HashedMap;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.file.processing.exception.GenerationException;
import com.adp.esi.digitech.file.processing.generator.util.XMLGeneratorUtils;
import com.adp.esi.digitech.file.processing.model.DataSet;
import com.adp.esi.digitech.file.processing.model.Row;

import lombok.extern.slf4j.Slf4j;

@Service("stAXXMLGeneratorService")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class StAXXMLGeneratorService extends AbstractGeneratorService<Row, byte[]> {

	@Value("${fileprocessing.xml.defaultPlaceHolder:_plchldr_}")
	private String __defaultPlaceHolder__;
	
	@Autowired
	XMLOutputFactory factory;
	
	@Autowired
	XMLGeneratorUtils xmlGeneratorUtils;
	
	@Override
	public byte[] generate(JSONObject outputFileRule, Map<String,DataSet<Row>> dataSetMap) throws GenerationException {
		log.info("StAXXMLGeneratorService -> generate() Started XML Generation, uniqueId = {}", requestContext.getUniqueId());
		var dataSetName = (String) outputFileRule.get("dataSetName");
		var data = dataSetMap.get(dataSetName).getData();
		Map<String, String> prologMap = new HashedMap<>();
		
		if(outputFileRule.has("prolog") && outputFileRule.get("prolog") != null && outputFileRule.getJSONArray("prolog").length() > 0) {		
			JSONArray prologJsonArray = outputFileRule.getJSONArray("prolog");								
			prologMap = xmlGeneratorUtils.getPrologMap(prologJsonArray);
		}
		
		try {
			//ByteArrayOutputStream out = new ByteArrayOutputStream();
			StringWriter mainBuffer = new StringWriter();
			writeXML(mainBuffer, prologMap, data, outputFileRule);
			var mainData = mainBuffer.toString().replaceAll("&lt;", "<").replaceAll("&gt;", ">");
			var transformer = xmlGeneratorUtils.newTransformer(prologMap);			
			String prettyPrintXml = xmlGeneratorUtils.formatXML(mainData, transformer).replaceAll(__defaultPlaceHolder__, "");
			//String prettyPrintXml = formatXML(new String(out.toByteArray(), StandardCharsets.UTF_8), prologMap).replaceAll(__defaultPlaceHolder__, "");
			log.info("StAXXMLGeneratorService -> generate() Completed XML Generation, uniqueId = {}", requestContext.getUniqueId());
			return prettyPrintXml.getBytes();
		} catch (TransformerException | XMLStreamException e) {
			log.error("StAXXMLGeneratorService -> generate() Failed XML Generation, uniqueId = {}, message = {}", requestContext.getUniqueId(), e.getMessage());
			var generationException = new GenerationException(e.getMessage(), e.getCause());
			generationException.setRequestContext(requestContext);
			throw generationException;
		}
		
	}
	
	private void writeXML(StringWriter mainBuffer, Map<String, String> prologMap, List<Row> data, JSONObject outputFileRulesJson) throws XMLStreamException {
		//XMLOutputFactory output = XMLOutputFactory.newInstance();
		XMLStreamWriter writer = factory.createXMLStreamWriter(mainBuffer);		
		/*
		if(prologMap.containsKey(OutputKeys.ENCODING) && prologMap.containsKey(OutputKeys.VERSION))		
			writer.writeStartDocument(prologMap.get(OutputKeys.ENCODING), prologMap.get(OutputKeys.VERSION));
		*/
		if(prologMap.containsKey(OutputKeys.VERSION))
			writer.writeStartDocument(prologMap.get(OutputKeys.VERSION));
		else 
			writer.writeStartDocument();
		
		if(outputFileRulesJson.has("xmlTemplate")) {
			JSONObject xmlTemplateJson = outputFileRulesJson.getJSONObject("xmlTemplate");	
			
			//Starting root element
			writer.writeStartElement(xmlTemplateJson.getString("name"));
			if(xmlTemplateJson.has("attrs") && xmlTemplateJson.get("attrs") != null && xmlTemplateJson.getJSONArray("attrs").length() > 0) {
				JSONArray rootElementAttrArray = xmlTemplateJson.getJSONArray("attrs");
				rootElementAttrArray.forEach(item -> {
					JSONObject obj = (JSONObject)item;
					try {
						writer.writeAttribute(obj.getString("name"), obj.getString("value"));
					} catch (JSONException | XMLStreamException e) {
						log.error("StAXXMLGeneratorService -> writeXML() Failed XML Generation, uniqueId = {}, message = {}", requestContext.getUniqueId(), e.getMessage());
					}
					
				});
			}
			if(xmlTemplateJson.has("child") && xmlTemplateJson.get("child") != null && xmlTemplateJson.getJSONArray("child").length() > 0) {
				JSONArray childElementArray = xmlTemplateJson.getJSONArray("child");
				childElementArray.forEach(item -> {
					JSONObject obj = (JSONObject)item;
					for (Row row : data) {
						xmlGeneratorUtils.writeElement(row, obj,writer,0,isTransformationRequired);
					}
				});
			}
			
			//end root element
			writer.writeEndElement();
		}	
		
		writer.writeEndDocument();
		writer.flush();
		writer.close();
	}
	
	
}
