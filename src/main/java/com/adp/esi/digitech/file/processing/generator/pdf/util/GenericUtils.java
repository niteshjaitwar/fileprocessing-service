package com.adp.esi.digitech.file.processing.generator.pdf.util;

import java.util.List;
import java.util.Objects;

import org.springframework.beans.factory.annotation.Autowired;

import com.adp.esi.digitech.file.processing.generator.document.config.model.Config;
import com.adp.esi.digitech.file.processing.generator.document.config.model.Font;
import com.adp.esi.digitech.file.processing.generator.document.pdf.element.service.PDFParagraphBuilder;
import com.adp.esi.digitech.file.processing.util.ValidationUtil;
import com.lowagie.text.Cell;
import com.lowagie.text.Chunk;
import com.lowagie.text.Document;
import com.lowagie.text.Element;
import com.lowagie.text.Paragraph;
import com.lowagie.text.alignment.HorizontalAlignment;

public class GenericUtils {

	@Autowired
	PDFParagraphBuilder paraBuilder;
	
	public static void createLineSpace(Document doc) {
		Paragraph para=new Paragraph("");
		para.setSpacingAfter(PDFConfigValues.defaultLineSpaceSize);
		doc.add(para);
	}
	
	public static void createMultipleLinesSpace(Document doc, int lines) {
		for(var i=0;i<lines;i++) {
			createLineSpace(doc);
		}
	}
	
	public static void createPageSpace(Document doc) {
		doc.newPage();
	}
	
	public static int getElementAlignment(String alignment) {
		int elementAlignment=0;
		if(!ValidationUtil.isHavingValue(alignment)) {
			alignment="";
		}
		alignment=alignment.toLowerCase();
		switch(alignment) {
		case PDFConfigValues.right:
			elementAlignment=Element.ALIGN_RIGHT;
			break;
		case PDFConfigValues.left:
			elementAlignment=Element.ALIGN_LEFT;
			break;
		case PDFConfigValues.center:
			elementAlignment=Element.ALIGN_CENTER;
			break;
		case PDFConfigValues.justified:
			elementAlignment=Element.ALIGN_JUSTIFIED;
			break;
		default:
			elementAlignment=Element.ALIGN_CENTER;
			break;
		}
		return elementAlignment;
	}
	
	public static HorizontalAlignment getElementHorizontalAlignment(int alignment) {
		HorizontalAlignment elementHorizontalAlignment = null;
		switch(alignment) {
		case 0:
			elementHorizontalAlignment=HorizontalAlignment.LEFT;
			break;
			
		case 1:
			elementHorizontalAlignment=HorizontalAlignment.CENTER;
			break;
			
		case 2:
			elementHorizontalAlignment=HorizontalAlignment.RIGHT;
			break;
		default:
			elementHorizontalAlignment=HorizontalAlignment.CENTER;
			break;
		}
		return elementHorizontalAlignment;

	}
	
	public static HorizontalAlignment getHorizontalAlignment(String alignment) {
		if(!ValidationUtil.isHavingValue(alignment)) {
			alignment="";
		}
		alignment=alignment.toLowerCase();
		switch(alignment) {
		case PDFConfigValues.left:
			return HorizontalAlignment.LEFT;
		
		case PDFConfigValues.center:
			return HorizontalAlignment.CENTER;
		
		case PDFConfigValues.right:
			return HorizontalAlignment.RIGHT;
		
		default:
			return HorizontalAlignment.CENTER;
		}
	}
	
	public static boolean isValidList(List<?> list) {
		boolean status=false;
		if(Objects.nonNull(list) && list.size()>0) {
			status=true;
		}
		return status;
	}
	
	public static boolean isValidArray(Object[] list) {
		boolean status=false;
		if(Objects.nonNull(list) && list.length>0) {
			status=true;
		}
		return status;
	}
	
}
