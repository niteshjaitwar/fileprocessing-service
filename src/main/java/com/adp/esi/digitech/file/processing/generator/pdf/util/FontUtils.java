package com.adp.esi.digitech.file.processing.generator.pdf.util;

import java.awt.Color;
import java.util.Objects;

import com.adp.esi.digitech.file.processing.generator.document.config.model.Font;
import com.adp.esi.digitech.file.processing.util.ValidationUtil;
import com.lowagie.text.FontFactory;

public class FontUtils {

	public static com.lowagie.text.Font buildFont(Font fontConfig){
		com.lowagie.text.Font font=null;
		if(Objects.nonNull(fontConfig)) {
			font=getFont(fontConfig.getFamily());
			try{
				font.setColor(getColorWithHexaCode(fontConfig.getColor()));
			}catch(Exception e) {
				font.setColor(Color.black);
			}
//			if(ValidationUtil.isHavingValue(fontConfig.getFamily()))
//				font.setFamily(fontConfig.getFamily());	
			
			if(fontConfig.getSize()>0)
				font.setSize(fontConfig.getSize());
			
			if(ValidationUtil.isHavingValue(fontConfig.getStyle()))
				font.setStyle(com.lowagie.text.Font.getStyleValue(fontConfig.getStyle().toLowerCase()));
			
		}else {
			font=new com.lowagie.text.Font(); 
		}
		return font;
	}
	
	public static Color getColorWithHexaCode(String hexacode) {
		if(ValidationUtil.isHavingValue(hexacode)) {
			try {
				return Color.decode(hexacode);
			}catch(Exception e) {
				return Color.black;
			}
			
		}else {
			return Color.black;
		}
	}
	
	private static com.lowagie.text.Font getFont(String fontName) {
		return FontFactory.getFont(fontName);
	}
}
