package com.adp.esi.digitech.file.processing.generator.pdf.util;

import com.adp.esi.digitech.file.processing.generator.document.config.model.Paragraph;
import com.adp.esi.digitech.file.processing.generator.document.pdf.element.service.PDFParagraphBuilder;
import com.adp.esi.digitech.file.processing.model.DataSet;
import com.adp.esi.digitech.file.processing.model.Row;
import com.adp.esi.digitech.file.processing.util.ValidationUtil;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lowagie.text.Font;
import com.adp.esi.digitech.file.processing.generator.document.config.model.Chunk;
import com.adp.esi.digitech.file.processing.generator.document.config.model.Config;

import java.util.HashMap;

import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.beans.factory.annotation.Autowired;

public class ParagraphUtils {
	
	@Autowired
	PDFParagraphBuilder paraBuilder;
	
	private static final ObjectMapper mapper = new ObjectMapper();
	
	public static com.lowagie.text.Paragraph createParagraph(Paragraph paragraphConfig, DataSet<Row> data) {
		String paragraphText = replaceFields(paragraphConfig.getContent(),data);
		HashMap<String,com.lowagie.text.Chunk> chunksMap = createChunkMap(replaceChunkDynamicFields(paragraphConfig.getChunks(),data));
		return generateParagraph(paragraphText,paragraphConfig,chunksMap);		
	}
		
    public static com.lowagie.text.Paragraph generateParagraph(String paragraphText, Paragraph paragraphConfig, HashMap<String,com.lowagie.text.Chunk> chunksMap)
    {	
    	com.lowagie.text.Paragraph para = new com.lowagie.text.Paragraph(); 
//    	int alignment=0;
//    	Font paragraphFont=null;
//    	if(Objects.nonNull(paragraphConfig.getConfig())) {
//			paragraphFont = FontUtils.buildFont(paragraphConfig.getConfig().getFont());
//			alignment = GenericUtils.getElementAlignment(paragraphConfig.getConfig().getAlignment());
//			
//			para.setAlignment(alignment);
//			//indent
//			if(Objects.nonNull(paragraphConfig.getConfig().getIndent()) && paragraphConfig.getConfig().getIndent()>0)
//				para.setIndentationLeft(paragraphConfig.getConfig().getIndent());
//			
//			//line spacing
//			if(Objects.nonNull(paragraphConfig.getConfig().getLineSpacing()) && paragraphConfig.getConfig().getLineSpacing()>0)
//				para.setMultipliedLeading(paragraphConfig.getConfig().getLineSpacing());
//				
//    	}
		
		
		
//		Pattern pattern = Pattern.compile("\\$\\$(chunk\\d+)\\$\\$");
    	setConfig(para, paragraphConfig.getConfig());
		Pattern pattern = Pattern.compile("\\$\\$([a-zA-Z0-9 \\-_#&%@]+)\\$\\$");//(\$\w+)\[(\w*[a-z]\w*)\]
		Matcher match = pattern.matcher(paragraphText);
		int lastIndex =0;
		while(match.find()) {
			String beforeChunk = paragraphText.substring(lastIndex, match.start());
			para.add(new com.lowagie.text.Chunk(beforeChunk,getFont(paragraphConfig.getConfig())));
//			para.add(new com.lowagie.text.Chunk(beforeChunk,paragraphFont));
			if(Objects.nonNull(chunksMap)) {
				String chunkKey = match.group(1);
				if(chunksMap.containsKey(chunkKey)) {
					para.add(chunksMap.get(chunkKey));
				}
			}
			lastIndex = match.end();
		}
		if(lastIndex < paragraphText.length()) {
			String lastContent = paragraphText.substring(lastIndex);
			para.add(new com.lowagie.text.Chunk(lastContent,getFont(paragraphConfig.getConfig())));
//			para.add(new com.lowagie.text.Chunk(lastContent,paragraphFont));
		}
//		para.l
		return para;	
	}
    
	
	public static HashMap<String,com.lowagie.text.Chunk> createChunkMap (List<Chunk> chunks) {
		HashMap<String,com.lowagie.text.Chunk> chunksMap = new HashMap<>();
		if(Objects.nonNull(chunks)) {
			for(Chunk chunk: chunks) {
				chunksMap.put(chunk.getName(),ChunkBuilder.create(chunk.getContent(), chunk.getConfig().getFont()));
			}
		}
		return chunksMap;
	}
	
//	public static String replaceFields(String paragraphText,List<DataSetMapping> dataSetMapping, boolean isPreview) {
//		HashMap<String,String> fieldMap = new HashMap<>(); 
//		for(DataSetMapping mapping: dataSetMapping) { 
//			fieldMap.put(mapping.getField(), isPreview ? mapping.getDefaultValue():mapping.getName()); 
//		}
//		
//		for(String field:fieldMap.keySet()) { 
//			paragraphText = paragraphText.replace("{{"+field+"}}", fieldMap.get(field)); 
//		}
//		return paragraphText;
//	}
	
	public static void setConfig(com.lowagie.text.Paragraph para, Config config) {
		int alignment=0;
		if(Objects.nonNull(config)) {
//			paragraphFont = FontUtils.buildFont(config.getFont());
			alignment = GenericUtils.getElementAlignment(config.getAlignment());

			//indent
			if(Objects.nonNull(config.getIndent()) && config.getIndent()>0)
				para.setIndentationLeft(config.getIndent());
			
			//line spacing
			if(Objects.nonNull(config.getLineSpacing()) && config.getLineSpacing()>0)
				para.setMultipliedLeading(config.getLineSpacing());
    	}
		para.setAlignment(alignment);
	}
	
	public static Font getFont(Config config) {
		Font font=new Font();
		if(Objects.nonNull(font)) {
			font=FontUtils.buildFont(config.getFont());
		}
		return font;
	}
	
	public static String replaceFields(String paragraphText,DataSet<Row> data) {
		if(isDataSetRowValid(data, 0)) {
			var columns=data.getData().get(0).getColumns();
			for(var column:columns.entrySet()) {
				var name=column.getValue().getName();
				var value="";
				if(Objects.nonNull(column.getValue().getTargetValue())) {
					value=column.getValue().getTargetValue();
				}
				paragraphText=paragraphText.replaceAll("\\{\\{"+name+"\\}\\}",value );
			}
		}
		
		return paragraphText;
	}
	
	private static boolean isDataSetRowValid(DataSet<Row> data,int index) {
		var status=false;
		if(Objects.nonNull(data) && Objects.nonNull(data.getData()) && Objects.nonNull(data.getData().get(index))) {
			status=true;
		}
		return status;
	}
	

	public static com.lowagie.text.Paragraph getSimpleParagraph(String text, Config config) {
		var para=new Paragraph();
		if(ValidationUtil.isHavingValue(text)) {
			para.setContent(text);
		}else {
			para.setContent("");
		}
		
		if(Objects.nonNull(config)) {
			para.setConfig(config);
		}
		
		return generateParagraph(text, para, null);
	}
	
	public static List<Chunk> replaceChunkDynamicFields(List<Chunk> chunks, DataSet<Row> data) {
		try {
			String objStr=mapper.writeValueAsString(chunks);
			objStr=replaceFields(objStr, data);
			chunks= mapper.readValue(objStr,  new TypeReference<List<Chunk>>(){});
		}catch(Exception e) {
			
		}
		return chunks;
	}
}