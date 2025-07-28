package com.adp.esi.digitech.file.processing.generator.util;

import java.io.File;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.collections4.map.HashedMap;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.file.processing.exception.GenerationException;
import com.adp.esi.digitech.file.processing.model.Row;
import com.adp.esi.digitech.file.processing.util.ValidationUtil;

import lombok.extern.slf4j.Slf4j;


@Service
@Slf4j
public class XMLGeneratorUtils {
	
	@Value("${fileprocessing.xml.defaultPlaceHolder:_plchldr_}")
	private String __defaultPlaceHolder__;
	
	@Autowired
	XMLOutputFactory factory;
	
	public boolean writeElement(Row row, JSONObject jsonObj, XMLStreamWriter writer, int depth, String isTransformationRequired)  {
		try {
			if(jsonObj.has("child") && jsonObj.get("child") != null && jsonObj.getJSONArray("child").length() > 0) {
				String isEmptyTagRequired = jsonObj.has("isEmptyTagRequired") && !jsonObj.isNull("isEmptyTagRequired") && ValidationUtil.isHavingValue(jsonObj.getString("isEmptyTagRequired")) ? jsonObj.getString("isEmptyTagRequired") : "";
				
				StringWriter buffer = new StringWriter();
				XMLStreamWriter bufferWriter = factory.createXMLStreamWriter(buffer);
				bufferWriter.writeStartElement(jsonObj.getString("name"));
				writeAttributes(row, jsonObj, bufferWriter, isTransformationRequired);
								
				JSONArray childElementArray = jsonObj.getJSONArray("child");
				boolean isChildFound = IntStream.range(0, childElementArray.length()).mapToObj(index -> writeElement(row, childElementArray.getJSONObject(index), bufferWriter, depth+1, isTransformationRequired)).reduce(false, Boolean::logicalOr);

				bufferWriter.writeEndElement();
				bufferWriter.flush();
				bufferWriter.close();
				
				boolean isGenerateTag = true;
				if(!isChildFound && !isEmptyTagRequired.equalsIgnoreCase("Y")) 
					isGenerateTag = false;
				
				if(isGenerateTag) {
					var data = buffer.toString();
					data = data.replaceAll("&lt;", "<").replaceAll("&gt;", ">");								
					writer.writeCharacters(data);		
					return isGenerateTag;
				}
				
			} else {
				if(jsonObj.has("value")) {
					String selfEndRequired = jsonObj.has("selfEndRequired") && !jsonObj.isNull("selfEndRequired") && ValidationUtil.isHavingValue(jsonObj.getString("selfEndRequired")) ? jsonObj.getString("selfEndRequired") : "";			
					String isEmptyTagRequired = jsonObj.has("isEmptyTagRequired") && !jsonObj.isNull("isEmptyTagRequired") && ValidationUtil.isHavingValue(jsonObj.getString("isEmptyTagRequired")) ? jsonObj.getString("isEmptyTagRequired") : "";
					String value = jsonObj.getString("value");
					
					if(value.startsWith("{{") && value.endsWith("}}")) {
						String col = value.substring(2, value.length()-2);					
						UUID key = UUID.fromString(col);
						if(!row.getColumns().containsKey(key)) {
							value = "";
							log.error("StAXXMLGeneratorService -> writeElement() column not found for the given key = {} ", col);
						} else {
							var column = row.getColumns().get(key);							
							value = isTransformationRequired.equalsIgnoreCase("N") ? column.getSourceValue() : column.getTargetValue();							
						}
								
					}
					
					boolean isGenerateTag = true;
					if(!ValidationUtil.isHavingValue(value) && !isEmptyTagRequired.equalsIgnoreCase("Y")) 
						isGenerateTag = false;
					
					if(isGenerateTag) {
						writer.writeStartElement(jsonObj.getString("name"));
						writeAttributes(row, jsonObj, writer, isTransformationRequired);
						writeCharacter(value, writer,selfEndRequired);	
						writer.writeEndElement();
						return isGenerateTag;
					}
					
				}
			}
			
			
		} catch (XMLStreamException e) {
			log.error("XMLGeneratorUtils -> writeElement(), error message = {}",e.getMessage());
			throw new GenerationException(e.getMessage(), e);
		}
		return false;
	}
	
	private void writeAttributes(Row row, JSONObject jsonObj, XMLStreamWriter writer, String isTransformationRequired) {
		if(jsonObj.has("attrs") && jsonObj.get("attrs") != null && jsonObj.getJSONArray("attrs").length() > 0) {
			JSONArray attrArray = jsonObj.getJSONArray("attrs");
			attrArray.forEach(item -> {
				JSONObject obj = (JSONObject)item;
		
				String value = obj.getString("value");
				if(value.startsWith("{{") && value.endsWith("}}")) {
					String col = value.substring(2, value.length()-2);
					UUID key = UUID.fromString(col);
					if(!row.getColumns().containsKey(key)) {
						log.error("generateXMLElement() attrs -> col = {} ", col);
					} else {
						var column = row.getColumns().get(key);
						var columnData = isTransformationRequired.equalsIgnoreCase("N") ? column.getSourceValue() : column.getTargetValue();
						writeAttribute(obj.getString("name"), columnData, writer);
					}
				} else 				
					writeAttribute(obj.getString("name"), value, writer);
			});
		} 
	}
	
	private void writeAttribute(String name, String value, XMLStreamWriter writer) {
		try {
			writer.writeAttribute(name, value != null ? value : "");
		} catch (XMLStreamException e) {
			log.error("XMLGeneratorUtils -> writeAttribute(), error message = {}",e.getMessage());
		}
	}
	
	private void writeCharacter(String value, XMLStreamWriter writer, String selfEndRequired) throws XMLStreamException {
		if(ValidationUtil.isHavingValue(value))	{	
			writer.writeCharacters(value);	
			return;
		}
		String defValue =	selfEndRequired.equalsIgnoreCase("Y") ? "" : __defaultPlaceHolder__;
		writer.writeCharacters(defValue);
		
	}
	
	public Transformer newTransformer(Map<String, String> prologMap) throws TransformerException {
		TransformerFactory factory = TransformerFactory.newDefaultInstance();
		Transformer transformer = factory.newTransformer();
		
		transformer.setOutputProperty(OutputKeys.INDENT, "yes");
		
		if(prologMap.containsKey(OutputKeys.STANDALONE))
			transformer.setOutputProperty(OutputKeys.STANDALONE, prologMap.get(OutputKeys.STANDALONE));
		
		if(prologMap.containsKey(OutputKeys.ENCODING))		
			transformer.setOutputProperty(OutputKeys.ENCODING, prologMap.get(OutputKeys.ENCODING));
		
		if(prologMap.containsKey(OutputKeys.METHOD))		
			transformer.setOutputProperty(OutputKeys.METHOD, prologMap.get(OutputKeys.METHOD));
		
		
		if(prologMap.containsKey(OutputKeys.MEDIA_TYPE))		
			transformer.setOutputProperty(OutputKeys.MEDIA_TYPE, prologMap.get(OutputKeys.MEDIA_TYPE));
		
		//transformer.setOutputProperty((String)obj.get("name"), (String)obj.get("value"));
		
		return transformer;
	}
	
	public void formatXML(File input, File output, Transformer transformer) throws TransformerException {
		StreamSource source = new StreamSource(input);
		StreamResult result = new StreamResult(output);
		transformer.transform(source, result);
	}
	
	public String formatXML(String xml, Transformer transformer) throws TransformerException {		
		StreamSource source = new StreamSource(new StringReader(xml));
		StringWriter output = new StringWriter();
		transformer.transform(source, new StreamResult(output));
		return output.toString();
	}
	
	
	public Map<String, String> getPrologMap(JSONArray prologJsonArray) {
		Map<String, String> prologMap = new HashedMap<>();
		prologJsonArray.forEach(item -> {
			JSONObject obj = (JSONObject)item;
			prologMap.put(obj.getString("name"), obj.getString("value"));
			
		});
		return prologMap;
	}

}
