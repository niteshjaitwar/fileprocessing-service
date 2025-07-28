package com.adp.esi.digitech.file.processing.generator.pdf.util;

import java.io.IOException;
import java.util.Base64;
import java.util.Objects;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import com.adp.esi.digitech.file.processing.generator.document.config.model.Config;
import com.lowagie.text.BadElementException;
import com.lowagie.text.Image;


public class ImageUtils {
	
	@Autowired
	ApplicationContext context;
	
	public static Image getImageElement(byte[] imageByteData,Config config) {
		Image img=null;
		try {
			img=Image.getInstance(imageByteData);
			if(Objects.nonNull(config)) {
				img.scaleAbsoluteHeight(config.getHeight());
				img.scaleAbsoluteWidth(config.getWidth());
				img.setAlignment(GenericUtils.getElementAlignment(config.getAlignment()));
			}
			
		} catch (BadElementException e) {
			throw new IllegalArgumentException("Not supported element");
		} catch (IOException e) {
			throw new IllegalArgumentException("Not supported dada type");
		}
		return img;
	}
	
	
	public static byte[] convertBase64ToByteArray(String base64) {
		return Base64.getDecoder().decode(base64);
	}
	
}
