package com.adp.esi.digitech.file.processing.generator.document.pdf.element.service;

import java.awt.Color;
import java.util.Objects;

import org.springframework.stereotype.Component;

import com.adp.esi.digitech.file.processing.generator.document.config.model.Element;
import com.adp.esi.digitech.file.processing.generator.document.config.model.SConfig;
import com.adp.esi.digitech.file.processing.generator.document.config.model.Shape;
import com.adp.esi.digitech.file.processing.generator.document.element.IElementBuilder;
import com.adp.esi.digitech.file.processing.model.DataSet;
import com.adp.esi.digitech.file.processing.model.Row;
import com.lowagie.text.Document;
import com.lowagie.text.Rectangle;

@Component("pdfshape")
public class PDFShapeBuilder implements IElementBuilder<Document>{

	@Override
	public void  build(Document doc, Element element, DataSet<Row> data) {
		var shape = (Shape)element.getComponent();
		generateShape(doc, shape);
	}

	@Override
	public com.lowagie.text.Element getElement(Element element, DataSet<Row> data) {
		// This element cannot be retrieved
		return null;
	}
	
	private void generateShape(Document doc,Shape shape) {
		if(Objects.nonNull(shape)) {
//			switch(ShapeType.valueOf(shape.getShapeType())) {
//			case Box:
				generateBox(doc, shape.getConfig());
//				break;
//			default:
//				throw new IllegalArgumentException("Not valid shape");
		}
	}
	

	
	public void generateBox(Document doc,SConfig config) {
		if(isBoxConfigValid(config)) {
			float ulx=config.getStartXPosition();
			float uly=config.getStartYPosition();
			float urx=ulx+config.getWidth();
			float ury=uly;
			float llx=ulx;
			float lly=uly-config.getHeight();
			
			Rectangle rect=new Rectangle(llx, lly, urx, ury);
			rect.setBorder(15);//to enable all sides
			rect.setBorderWidth(config.getBorderDensity());
			rect.setBorderColor(Color.decode(config.getBorderColor()));
			doc.add(rect);
		}
	}
	
	private boolean isBoxConfigValid(SConfig config) {
		boolean status=false;
		if(Objects.nonNull(config) &&  (config.getWidth()>0 || config.getHeight()>0)) {
			status=true;
		}
		return status;
	}

}
