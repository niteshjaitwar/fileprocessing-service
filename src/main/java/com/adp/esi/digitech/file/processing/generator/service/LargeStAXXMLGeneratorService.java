package com.adp.esi.digitech.file.processing.generator.service;

import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

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
import com.adp.esi.digitech.file.processing.util.ValidationUtil;
import com.fasterxml.jackson.core.type.TypeReference;

import lombok.extern.slf4j.Slf4j;

@Service("largeStAXXMLGeneratorService")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class LargeStAXXMLGeneratorService extends AbstractLargeGeneratorService<Void, Void> {
	
	@Value("${fileprocessing.xml.defaultPlaceHolder:_plchldr_}")
	private String __defaultPlaceHolder__;
	
	@Autowired
	XMLOutputFactory factory;
	
	@Autowired
	XMLGeneratorUtils xmlGeneratorUtils;
	
	@Override
	public Void generate(JSONObject outputFileRule, Map<String, DataSet<Void>> data) throws GenerationException {
		log.info("LargeStAXXMLGeneratorService -> generate() Started XML Generation, uniqueId = {}", requestContext.getUniqueId());
		
		var requestDir = largeRequestFilePath + requestContext.getRequestUuid();
		var dataSetDir = requestDir + "/datasets";		
		var dataSetId = outputFileRule.getString("dataSetName");		
		var currentDataSetTransformDir = Paths.get(dataSetDir + "/" + dataSetId + "/transform");
		
		Map<String, String> prologMap = new HashedMap<>();
		
		if(outputFileRule.has("prolog") && outputFileRule.get("prolog") != null && outputFileRule.getJSONArray("prolog").length() > 0) {		
			JSONArray prologJsonArray = outputFileRule.getJSONArray("prolog");								
			prologMap = xmlGeneratorUtils.getPrologMap(prologJsonArray);
		}
		
		try {
			writeXML(outputFileRule, currentDataSetTransformDir, prologMap);
		} catch (IOException e) {
			throw new GenerationException(e.getMessage(), e);
		}
		
		return null;
	}
	
	private void writeXML(JSONObject outputFileRule, Path currentDataSetTransformDir, Map<String, String> prologMap) throws IOException {
		var fileName = constructFileName(outputFileRule);
		var outputPath = getOutputPath(fileName, "xml");
		var tempOutputPath = getOutputPath("temp_" + fileName, "xml");
		
		try(Stream<Path> paths = Files.list(currentDataSetTransformDir).filter(path -> path.toFile().isFile());
			FileOutputStream fileOut = new FileOutputStream(tempOutputPath.toFile());
			var channel = FileChannel.open(tempOutputPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)){
			var transformer = xmlGeneratorUtils.newTransformer(prologMap);
					
			XMLStreamWriter writer = factory.createXMLStreamWriter(fileOut);
			
			if(prologMap.containsKey(OutputKeys.VERSION))
				writer.writeStartDocument(prologMap.get(OutputKeys.VERSION));
			else 
				writer.writeStartDocument();
			
			if(outputFileRule.has("xmlTemplate")) {
				JSONObject xmlTemplateJson = outputFileRule.getJSONObject("xmlTemplate");	
				
				//Starting root element
				writer.writeStartElement(xmlTemplateJson.getString("name"));
				if(xmlTemplateJson.has("attrs") && xmlTemplateJson.get("attrs") != null && xmlTemplateJson.getJSONArray("attrs").length() > 0) {
					JSONArray rootElementAttrArray = xmlTemplateJson.getJSONArray("attrs");
					rootElementAttrArray.forEach(item -> {
						JSONObject obj = (JSONObject)item;
						try {
							writer.writeAttribute(obj.getString("name"), obj.getString("value"));
						} catch (JSONException | XMLStreamException e) {
							log.error("LargeStAXXMLGeneratorService -> writeXML() Failed XML Generation, uniqueId = {}, message = {}", requestContext.getUniqueId(), e.getMessage());
						}
						
					});
				}
				
				if(xmlTemplateJson.has("child") && xmlTemplateJson.get("child") != null && xmlTemplateJson.getJSONArray("child").length() > 0) {
					JSONArray childElementArray = xmlTemplateJson.getJSONArray("child");
					childElementArray.forEach(item -> {
						JSONObject elementRulesJson = (JSONObject)item;
						paths.forEach(path -> {
							try {
								var sReader = new FileReader(path.toFile());
								var rows =objectMapper.readValue(sReader, new TypeReference<List<Row>>() {});
								
								StringWriter mainBuffer = new StringWriter();								
								writeXML(mainBuffer, prologMap, rows, elementRulesJson);								
								
								var mainData = mainBuffer.toString().replaceAll("&lt;", "<").replaceAll("&gt;", ">");
								String prettyPrintXml = xmlGeneratorUtils.formatXML(mainData, transformer).replaceAll(__defaultPlaceHolder__, "");
								
								var mainContent = getContent(prettyPrintXml);								
								//log.info("mainContent = {}",mainContent);
								
								writer.writeCharacters(mainContent);
								writer.writeCharacters("\n");								
								
							} catch(XMLStreamException | TransformerException | IOException e) {
								throw new GenerationException(e.getMessage(), e);
							}
						});
						
					});
				}
				
				//end root element
				writer.writeEndElement();
			}	
			
			writer.writeEndDocument();
			writer.flush();
			writer.close();
			fileOut.flush();
			fileOut.close();
			
			var  streamLines = Files.lines(tempOutputPath);
			var bufferWriter = Files.newBufferedWriter(outputPath);
			streamLines.filter(line -> ValidationUtil.isHavingValue(line))
						.map(line -> line.replaceAll("&lt;", "<")
										 .replaceAll("&gt;", ">")
										 .replaceAll("&#xd;", "")
										 .replaceAll("&amp;", "&")
										 .replaceAll("&quot;", "\""))
						.forEach(line -> {
							try {
								bufferWriter.write(line);
								bufferWriter.newLine();
							} catch (IOException e) {
								 throw new GenerationException(e.getMessage(), e);
							}
						});
			
			Files.delete(tempOutputPath);
			streamLines.close();
			bufferWriter.close();
		} catch (TransformerException | XMLStreamException e) {
            throw new GenerationException(e.getMessage(), e);
		}
		
		
	}
	
	private void writeXML(StringWriter mainBuffer, Map<String, String> prologMap, List<Row> data, JSONObject elementRulesJson) throws XMLStreamException {		
		XMLStreamWriter writer = factory.createXMLStreamWriter(mainBuffer);	
		writer.writeStartElement("temp");
		
		for (Row row : data) {
			xmlGeneratorUtils.writeElement(row, elementRulesJson,writer,0,isTransformationRequired);
		}	
		writer.writeEndElement();
		writer.flush();
		writer.close();
	}
	
	private String getContent(String mainContent) {
		String startTag = "<temp>";
		String endTag = "</temp>";		
		return mainContent.substring((mainContent.indexOf(startTag) + startTag.length()), mainContent.indexOf(endTag));
	}

}
