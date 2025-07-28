package com.adp.esi.digitech.file.processing.generator.pdf.util;

import java.util.Map;
import java.util.Objects;

import java.util.HashMap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.adp.esi.digitech.file.processing.generator.document.config.model.Config;
import com.adp.esi.digitech.file.processing.generator.document.config.model.Content;
import com.adp.esi.digitech.file.processing.generator.document.config.model.HeaderFooter;
import com.adp.esi.digitech.file.processing.generator.document.config.model.Image;
import com.adp.esi.digitech.file.processing.generator.document.config.model.PageNo;
import com.adp.esi.digitech.file.processing.generator.document.config.model.SConfig;
import com.adp.esi.digitech.file.processing.generator.document.pdf.element.service.PDFImageBuilder;
import com.adp.esi.digitech.file.processing.generator.document.pdf.element.service.PDFShapeBuilder;
import com.adp.esi.digitech.file.processing.generator.document.pdf.service.ImageRetrievalService;
import com.adp.esi.digitech.file.processing.util.ValidationUtil;
import com.lowagie.text.Cell;
import com.lowagie.text.Document;
import com.lowagie.text.DocumentException;
import com.lowagie.text.Element;
import com.lowagie.text.Table;
import com.lowagie.text.pdf.PdfWriter;


@Component
public class HeaderFooterUtils {
	
	@Autowired
	ImageRetrievalService imageSvc;
	
	@Autowired
	PDFImageBuilder imgBuilder;
	
	@Autowired
	PDFShapeBuilder shapeBuilder;
	
	public void createHeaderFooter(PdfWriter pw, Document document, HeaderFooter header, HeaderFooter footer) {
		try {
			if(Objects.nonNull(header))
				createHFOnPage(pw,document,header,(document.top()+((float)(document.topMargin()*PDFConfigValues.headerSetBackPerCent))));//for header
				if(Objects.nonNull(header.getConfig()))
					createHFLine(document, header.getConfig(), (document.getPageSize().getHeight()-document.topMargin()+2));
			
			if(Objects.nonNull(footer))
				createHFOnPage(pw,document,footer, ((float)(document.bottomMargin()*PDFConfigValues.footerSetBackPerCent)));//for footer
				if(Objects.nonNull(footer.getConfig()))
					createHFLine(document, footer.getConfig(), (document.bottomMargin()-2));
			
		} catch(DocumentException e) {
			throw new RuntimeException(e);
		}
	} 
	
	private void createHFOnPage(PdfWriter pw, Document doc, HeaderFooter hf, float y) {
		float tableWidth=getHFTableWidth(doc);
		TableUtils.convertToPdfPTable(headerFooterTable(hf,tableWidth,pw),tableWidth).
		writeSelectedRows(0, -1, doc.left(), y, pw.getDirectContent());
	}
	
	private Table headerFooterTable(HeaderFooter headerFooter, float tableWidth,PdfWriter pw) {
		try {
			float cellWidth=tableWidth/3;		
			Cell[][] cells=getEmptyCellsForHF(cellWidth);
			rearrangeElmWithAlignment(getHeaderFooterElements(pw, headerFooter), cells, cellWidth);
			return TableUtils.getTable(cells, null,PDFConfigValues.listTableColumnRatio);
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}			
		
	}
	
	private void createHFLine(Document doc,SConfig config, float yPosition) {
		config.setStartXPosition(doc.leftMargin());
		config.setStartYPosition(yPosition);
		config.setWidth(getHFLineWidth(doc));
		if(!ValidationUtil.isHavingValue(config.getBorderColor())){
			config.setBorderColor(PDFConfigValues.blackColor);
		}
		shapeBuilder.generateBox(doc, config);
	}
	
//	private void createFooterLine(Document doc,SConfig config) {
//		config.setStartXPosition(doc.leftMargin());
//		config.setStartXPosition(doc.topMargin()+2);
//		config.setWidth(getHFLineWidth(doc));
//	}
	
	private float getHFLineWidth(Document doc) {
		float width=(float)(doc.getPageSize().getWidth()-(doc.leftMargin()+doc.rightMargin()));
		return width;
	}
	
	private Cell[][] rearrangeElmWithAlignment(Map<String, com.lowagie.text.Element> elementsMap, Cell[][] cells, float cellWidth){
		
		elementsMap.forEach((alignment,element)->{
			if(Objects.nonNull(element)) {
				if(ValidationUtil.isHavingValue(alignment))
					alignment=alignment.toLowerCase();
				
				switch(alignment) {
				case PDFConfigValues.left:
					cells[0][0]=createCellWithHFElm(element, alignment, cellWidth);
					break;
				case PDFConfigValues.center:
					cells[0][1]=createCellWithHFElm(element, alignment, cellWidth);
					break;
				case PDFConfigValues.right:
					cells[0][2]=createCellWithHFElm(element, alignment, cellWidth);
					break;
				default:
					cells[0][1]=createCellWithHFElm(element, alignment, cellWidth);
					break;
				}
			}
		});
		return cells;
	}
	
	private Cell createCellWithHFElm(com.lowagie.text.Element element, String alignment, float cellWidth) {
		return TableUtils.createCell(element, cellWidth, false, PDFConfigValues.whiteColor, 0, GenericUtils.getHorizontalAlignment(alignment),true,PDFConfigValues.bottom);
	}
	
	private Cell[][] getEmptyCellsForHF(float cellWidth) {
		Cell[][] cells=new Cell[1][3];
		for(int i=0;i<3;i++) {
			cells[0][i] = TableUtils.createCell(ChunkBuilder.create("",null), cellWidth, false, PDFConfigValues.whiteColor, 0, GenericUtils.getElementHorizontalAlignment(i),true,PDFConfigValues.bottom);	
		}
		return cells;
	}
	
	private float getHFTableWidth(Document doc) {
		return (doc.right()-doc.left());
	}
	
	private float getHFCellWidth(Document doc) {
		return getHFTableWidth(doc)/3;
	}
	
	private Map<String, com.lowagie.text.Element> getHeaderFooterElements(PdfWriter pw,HeaderFooter headerFooter){
		Map<String,com.lowagie.text.Element> elementsMap= new HashMap<>(); 
		getHFElementMap(pw, elementsMap, headerFooter.getImage());
		getHFElementMap(pw, elementsMap, headerFooter.getContent());
		getHFElementMap(pw, elementsMap, headerFooter.getPageNo());
//		elementsMap.put(getConfigAlignment(headerFooter.getImage().getConfig()),getHFImage(headerFooter.getImage()));
//		elementsMap.put(getConfigAlignment(headerFooter.getContent().getConfig()),getHFContent(headerFooter.getContent()));
//		elementsMap.put(getConfigAlignment(headerFooter.getPageNo().getConfig()),getHFPageNo(pw,headerFooter.getPageNo()));
//		System.out.println(elementsMap);
		return elementsMap;
	}
	
	private <T> void getHFElementMap(PdfWriter pw,Map<String,com.lowagie.text.Element> elementsMap,T t) {
		if(Objects.nonNull(t)) {
			if(t instanceof Image) {
				var image=(Image)t;
				elementsMap.put(getConfigAlignment(image.getConfig()), getHFImage(image));
			}else if(t instanceof Content) {
				var content=(Content)t;
				elementsMap.put(getConfigAlignment(content.getConfig()), getHFContent(content));
			}else if(t instanceof PageNo) {
				var pageNo=(PageNo)t;
				elementsMap.put(getConfigAlignment(pageNo.getConfig()), getHFPageNo(pw,pageNo));
			}
		}else {
//			elementsMap.put("", ParagraphUtils.getSimpleParagraph("", null));
		}
	}
	
	private String getConfigAlignment(Config config) {
		if(Objects.nonNull(config) && ValidationUtil.isHavingValue(config.getAlignment())) {
			return config.getAlignment();
		}else {
			return "";
		}
	}
	
	private com.lowagie.text.Element getHFImage(com.adp.esi.digitech.file.processing.generator.document.config.model.Image image) {
		if(Objects.nonNull(image)) {
			return getImageWithBuilder(image);
		}else {
			return ParagraphUtils.getSimpleParagraph("", null);
		}
	}
	
	private com.lowagie.text.Element getHFPageNo(PdfWriter pw,PageNo pageNo) {
		if(Objects.nonNull(pageNo)){
			String pageTxt=pageNo.getText();
			if(Objects.isNull(pageTxt)){
				pageTxt="";
			}
			pageTxt+=pw.getPageNumber();
			return ParagraphUtils.getSimpleParagraph(pageTxt, pageNo.getConfig());
		}else {
			return ParagraphUtils.getSimpleParagraph("", null);
		}
	}
	
	private com.lowagie.text.Element getHFContent(Content content) {
		if(Objects.nonNull(content)) {
			return ParagraphUtils.getSimpleParagraph(content.getText(), content.getConfig());
		}else {
			return ParagraphUtils.getSimpleParagraph("", null);
		}
	}
	

	
	
	private Element getImageWithBuilder(Image image) {
		com.adp.esi.digitech.file.processing.generator.document.config.model.Element element=new com.adp.esi.digitech.file.processing.generator.document.config.model.Element();
		element.setComponent(image);
		return imgBuilder.getElement(element, null);
	}
	

	
}