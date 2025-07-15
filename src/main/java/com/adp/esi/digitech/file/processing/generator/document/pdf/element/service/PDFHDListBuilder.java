package com.adp.esi.digitech.file.processing.generator.document.pdf.element.service;

import java.util.Objects;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.adp.esi.digitech.file.processing.generator.document.config.model.Config;
import com.adp.esi.digitech.file.processing.generator.document.config.model.Element;
import com.adp.esi.digitech.file.processing.generator.document.config.model.Font;
import com.adp.esi.digitech.file.processing.generator.document.config.model.HDConfig;
import com.adp.esi.digitech.file.processing.generator.document.config.model.HDList;
import com.adp.esi.digitech.file.processing.generator.document.element.IElementBuilder;
import com.adp.esi.digitech.file.processing.generator.document.enums.BulletType;
import com.adp.esi.digitech.file.processing.generator.pdf.util.PDFConfigValues;
import com.adp.esi.digitech.file.processing.generator.pdf.util.TableUtils;
import com.adp.esi.digitech.file.processing.model.DataSet;
import com.adp.esi.digitech.file.processing.model.Row;
import com.lowagie.text.Document;
import com.lowagie.text.FontFactory;
import com.lowagie.text.Paragraph;
import com.lowagie.text.Table;

@Component("pdfhdlist")
public class PDFHDListBuilder implements IElementBuilder<Document>{

	@Autowired
	PDFElementRouterImpl router;

	@Autowired
	PDFParagraphBuilder paraBuilder;
	
	@Override
	public void build(Document doc, Element element, DataSet<Row> data) {
		doc.add(getElement(element, data));
	}
	
	@Override
	public com.lowagie.text.Element getElement(Element element, DataSet<Row> data) {
		var hdList =(HDList) element.getComponent();
		var elements = hdList.getElements();
		Table table = createList(elements, data, hdList.getConfig());
		return table;
	}
	
//	private void generatetElements(Element element, DataSet<Row> data,Document doc){
//		var hdList =(HDList) element.getComponent();
//		List list=getListElementWithInitialConfig(hdList.getConfig());
//		var elements=hdList.getElements();
////		hdList.get
//		System.out.println("elements length:"+elements.length);
//		System.out.println(elements);
//		
//		for(Element intElement:elements) {
//			var pdfElement=router.getElement(intElement, data);
//			doc.add(pdfElement);
//		}
//	}

//	private List getListElementWithInitialConfig(HDConfig listConfig) {
//		List list=new List();
//		System.out.println("bullet type:"+listConfig.getBulletType());
////		BulletType.
//		switch(BulletType.valueOf(listConfig.getBulletType())) {
//		case Number:
//			list.setNumbered(true);
//			break;
//		case Alphabet:
//			list.setLettered(true);
//			break;
//		case Symbol:
//			list.setPreSymbol(listConfig.getSymbol());
//			break;
//		case None:
//			//logic
//			break;
//		default:
//			//default type
//			break;
//		}
//		return list;
//	}
	
	private String getPreCharacter(String type,int number, String symbol) {
		String alphabets=PDFConfigValues.alphabets;
		if(Objects.isNull(symbol) || symbol.isBlank()) {
			symbol="";
		}
		switch(BulletType.valueOf(type)) {
		case Number:
			return ""+(number+1)+symbol;
		case Alphabet_Upper_Case:
			return String.valueOf(alphabets.toUpperCase().charAt(number))+symbol;
		case Alphabet_Lower_Case:
			return String.valueOf(alphabets.charAt(number))+symbol;
		case Unicode:
			return symbol;
		case Symbol:
			return symbol;
		case None:
			return "";
		default:
			return "";
		}
	}
	
	private void addEmptyRowSpace(Table table, int lines) {
		if(lines>0) {
//			String lineSpace="\n";
			String lineSpace="";
//			lineSpace.repeat(lines);
			for(var i=0;i<lines;i++) {
				table.addCell(TableUtils.createCell(new Paragraph(lineSpace), 0, false, PDFConfigValues.whiteColor, 0, PDFConfigValues.right, true));
				table.addCell(TableUtils.createCell(new Paragraph(""), 0, false, PDFConfigValues.whiteColor, 0, PDFConfigValues.right, true));
				table.addCell(TableUtils.createCell(new Paragraph(""), 0, false, PDFConfigValues.whiteColor, 0, PDFConfigValues.right, true));
			}
		}
	}
	
	private Table createList(Element[] elements, DataSet<Row> data, HDConfig config) {
		Table table=TableUtils.createListTable(3, config);
		var rowNo=0;
		addEmptyRowSpace(table, config.getTopLineSpace());
		for(Element intElement:elements) {
//			var elementAlignment=intElement.getComponent().
			var pdfElement=router.getElement(intElement, data);
			addRowWithElement(table, pdfElement,config, getPreCharacter(config.getBulletType(), rowNo, config.getSymbol()),config.getInternalLineSpace(), intElement.getType());
			rowNo++;
		}
		addEmptyRowSpace(table, config.getBottomLineSpace());
		return table;
	}
	
	private void addRowWithElement(Table table, com.lowagie.text.Element element,HDConfig config, String preChar, int internalLineSpace, String elementType) {
		if(elementType.equals("HDList")) {
			preChar="";
		}else {
			preChar+=" ";
		}
//		buildPreChar(elementType, null)
		table.addCell(TableUtils.createCell(buildPreChar(preChar,config), 0, false, PDFConfigValues.whiteColor, 0, PDFConfigValues.right, true, ""));
		table.addCell(TableUtils.createCell(element, 0, false, PDFConfigValues.whiteColor, 0, PDFConfigValues.justified, true, ""));
		table.addCell(TableUtils.createCell(new Paragraph(""), 0, false, PDFConfigValues.whiteColor, 0, PDFConfigValues.right, true, ""));
		addEmptyRowSpace(table, internalLineSpace);
	}
	
	private com.lowagie.text.Element buildPreChar(String content, HDConfig hdConfig) {
		Config config=new Config();
		Font font=new Font();
		config.setAlignment(PDFConfigValues.right);
		if(Objects.nonNull(hdConfig.getFont())){
			font=hdConfig.getFont();
		}
		if(hdConfig.getBulletType().equals(BulletType.Unicode.getBulletType())) {
			font.setFamily(FontFactory.defaultEncoding);
		}
		config.setFont(font);
		
		var para1=new com.adp.esi.digitech.file.processing.generator.document.config.model.Paragraph();
		para1.setContent(content);
		para1.setConfig(config);
		para1.setChunks(null);
		
		Element el=new Element();
		el.setComponent(para1);
		return paraBuilder.getElement(el, null);
	}
	
}
