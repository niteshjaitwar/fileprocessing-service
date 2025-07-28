package com.adp.esi.digitech.file.processing.generator.pdf.util;

import java.awt.Color;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.poi.hpsf.Array;

import com.adp.esi.digitech.file.processing.generator.document.config.model.HDConfig;
import com.adp.esi.digitech.file.processing.generator.document.config.model.TBody;
import com.adp.esi.digitech.file.processing.generator.document.config.model.TColumn;
import com.adp.esi.digitech.file.processing.generator.document.config.model.TConfig;
import com.adp.esi.digitech.file.processing.generator.document.config.model.THeader;
import com.adp.esi.digitech.file.processing.generator.document.enums.TransposeType;
import com.adp.esi.digitech.file.processing.util.ValidationUtil;
import com.lowagie.text.Cell;
import com.lowagie.text.Paragraph;
import com.lowagie.text.Table;
import com.lowagie.text.alignment.HorizontalAlignment;
import com.lowagie.text.alignment.VerticalAlignment;
import com.lowagie.text.pdf.PdfPTable;

public class TableUtils {

	
	public static Cell[][] joinHeaderANDBody(Cell[] headerRow, Cell[][] bodyRow, String isTranspose){
		Cell[][] newMatrix=null;
		int noRows=0;
		int noCols=0;
		
		if(Objects.nonNull(headerRow) && headerRow.length>0)
			noCols=headerRow.length;
		
		if(Objects.nonNull(bodyRow) && bodyRow.length>0 ) 
			if(isTranspose.equals(PDFConfigValues.tableTransposeY))
				noRows=bodyRow[0].length;
			else
				noRows=bodyRow.length;
		
		if(isTranspose.equals(TransposeType.Yes.getTranposeType())) {
			newMatrix=new Cell[noCols][noRows+1];
			for(var rowNo=0;rowNo<newMatrix.length;rowNo++) {
				for(var colNo=0;colNo<newMatrix[0].length;colNo++) {
					if(colNo==0) {
						newMatrix[rowNo][0]=headerRow[rowNo];
					}else {
						newMatrix[rowNo][colNo]=bodyRow[rowNo][colNo-1];
					}
					
				}
			}
		}else {
			newMatrix=new Cell[noRows+1][noCols];
			newMatrix[0]=headerRow;
			for(var rowNo=1;rowNo<newMatrix.length;rowNo++) {
				newMatrix[rowNo]=bodyRow[rowNo-1];
			}
		}
		return newMatrix;
	}
	public static List<Cell[][]> splitMatrix(Cell[][] matrix, int maxRows ) {
		ArrayList<Cell[][]> list=new ArrayList<Cell[][]>();
//		if(matrix.length>maxRows) {
		if(maxRows>0) {
			var tbRequired=(int)Math.ceil((float)matrix.length/(maxRows));
			var rowNo=0;
			for(var tableNo=0;tableNo<tbRequired;tableNo++) {

				var maxTableRows=maxRows;
				if(rowNo+maxRows>=matrix.length) {
					maxTableRows=matrix.length-rowNo;
				}
				Cell[][] table=new Cell[maxTableRows][matrix.length];
				System.arraycopy(matrix, rowNo, table, 0, maxTableRows);
				list.add(table);
				rowNo+=maxTableRows;
			}
		}else {
			list.add(matrix);
		}
//		}
		return list;
	}
	

	public static Cell createCell(String text,String bgColor,boolean disableBorders) {
		Cell cell=new Cell();
		cell.add(new Paragraph(text));
		cell.setBackgroundColor(Color.decode(bgColor));
		if(disableBorders) {
			cell.disableBorderSide(-1);
		}
		return cell;
	}
	
	
	public static Cell createCell(com.lowagie.text.Element element,float width, boolean isHeader,String bgColor, int colspan) {
		Cell cell=new Cell(element);
		cell.setWidth(width);
		cell.setHeader(isHeader);
		cell.setBackgroundColor(FontUtils.getColorWithHexaCode(bgColor));
		if(colspan<0)
			cell.setColspan(colspan);
		return cell;
	}
	
	public static Cell createCell(com.lowagie.text.Element element,float width, boolean isHeader,String bgColor, int colspan,boolean disableBorders) {
		Cell cell=new Cell(element);
		cell.setWidth(width);
		cell.setHeader(isHeader);
		cell.setBackgroundColor(FontUtils.getColorWithHexaCode(bgColor));
		if(disableBorders) {
			cell.disableBorderSide(-1);
		}
//		cell.setColspan(colspan);
		return cell;
	}
	
	public static Cell createCell(com.lowagie.text.Element element,float width, boolean isHeader,String bgColor, int colspan, String alignment) {
		Cell cell=createCell(element, width,  isHeader, bgColor,  colspan);
		cell.setHorizontalAlignment(HorizontalAlignment.of( GenericUtils.getElementAlignment(alignment)).get());
		return cell;
	}
	
	public static Cell createCell(com.lowagie.text.Element element,float width, boolean isHeader,String bgColor, int colspan, String alignment,boolean disableBorders) {
		Cell cell=createCell(element, width,  isHeader, bgColor,  colspan,disableBorders);
		cell.setHorizontalAlignment(HorizontalAlignment.of( GenericUtils.getElementAlignment(alignment)).get());
//		cell.setVerticalAlignment(VerticalAlignment.TOP);
		return cell;
	}
	
	
	public static Cell createCell(com.lowagie.text.Element element,float width, boolean isHeader,String bgColor, int colspan, String alignment,boolean disableBorders, String verticalAlignment) {
		Cell cell=createCell(element, width,  isHeader, bgColor,  colspan,disableBorders);
		cell.setHorizontalAlignment(HorizontalAlignment.of( GenericUtils.getElementAlignment(alignment)).get());
		cell.setVerticalAlignment(getVerticalAlignment(verticalAlignment));
		return cell;
	}
	
	public static Cell createCell(com.lowagie.text.Element element,float width, boolean isHeader,String bgColor, int colspan, HorizontalAlignment alignment,boolean disableBorders, String verticalAlignment) {
		Cell cell=createCell(element, width,  isHeader, bgColor,  colspan,disableBorders);
		cell.setHorizontalAlignment(alignment);
		cell.setVerticalAlignment(getVerticalAlignment(verticalAlignment));
		return cell;
	}
	
	public static Cell createCell(com.lowagie.text.Element element,float width, boolean isHeader,String bgColor, int colspan, int alignment,boolean disableBorders) {
		Cell cell=createCell(element, width,  isHeader, bgColor,  colspan,disableBorders);
		cell.setHorizontalAlignment(GenericUtils.getElementHorizontalAlignment(alignment));
		return cell;
	}
	
	public static Cell[][] transposeMatrix(Cell[][] cellMatrix){
		Cell[][] tMatrix=new Cell[cellMatrix[0].length][cellMatrix.length];
		for(var tRowNo=0;tRowNo<cellMatrix[0].length;tRowNo++) {
			for(var tColNo=0;tColNo<cellMatrix.length;tColNo++) {
				tMatrix[tRowNo][tColNo]=cellMatrix[tColNo][tRowNo];
			}
		}
		return tMatrix;
	}
	
	
	public static Cell[][] createCells(String[][] matrix){
		Cell[][] cells=new Cell[matrix.length][matrix[0].length];
		for(var rowNo=0;rowNo<matrix.length;rowNo++) {
			for(var colNo=0;colNo<matrix[0].length;colNo++) {
				cells[rowNo][colNo]=createCell(matrix[rowNo][colNo],PDFConfigValues.whiteColor,true);
			}
		}
		return cells;
	}
	

	
	public static Table getTable(Cell[][] matrix, TConfig config, float[] colWidthsRatio) {
		Table table=new Table(matrix[0].length);
		table.disableBorderSide(-1);//Disable Rectangular border outside

		if(Objects.nonNull(config)) {
			table.setPadding(config.getPadding());
			table.setWidth(config.getWidth());
			table.setHorizontalAlignment(HorizontalAlignment.of(GenericUtils.getElementAlignment(config.getAlignment())).get());
			if(Objects.nonNull(colWidthsRatio)) {
				table.setWidths(colWidthsRatio);
			}

		}
		
		for(var rowNo=0;rowNo<matrix.length;rowNo++) {
			for(var colNo=0;colNo<matrix[0].length;colNo++) {
				table.addCell(matrix[rowNo][colNo], rowNo, colNo);
			}
		}
		return table;
	}
	
	public static PdfPTable convertToPdfPTable(Table table, float width) {
		table.setConvert2pdfptable(true);
		PdfPTable pTable=table.createPdfPTable();
		pTable.setTotalWidth(width);
		return pTable;
	}
	
	public static Table createListTable(int colsRequired,HDConfig config) {
		
		Table table=new Table(colsRequired);
		table.setOffset(PDFConfigValues.defaultListTableOffset);
		table.setSpacing(0);
		if(Objects.nonNull(config)) {
			if(config.getWidth()>0) {
				table.setWidth(config.getWidth());
			}
			
			if(config.getBorderDensity()==0) {
				table.disableBorderSide(-1);
			}else {
				table.setBorderWidth(config.getBorderDensity());
				if(Objects.nonNull(config.getBorderColor()) && !config.getBorderColor().isBlank()) {
					table.setBorderColor(FontUtils.getColorWithHexaCode(config.getBorderColor()));
				}else {
					table.setBorderColor(FontUtils.getColorWithHexaCode(PDFConfigValues.blackColor));
				}
			}
			
			//widths
			float[] widths= PDFConfigValues.listTableColumnRatio;
			if(config.getBulletType().equals(PDFConfigValues.bulletTypeNone)) {
				widths=PDFConfigValues.listTableColumnRatioWithoutSymbol;
			}
			table.setWidths(widths);
			
//			table.setOffset(0);
//			table.setSpacing(0);
//			table.setPadding(PDFConfigValues.listTableDefaultPadding);
			if( Objects.nonNull(config.getAlignment()) && !config.getAlignment().isEmpty()) {
				table.setHorizontalAlignment(HorizontalAlignment.of(GenericUtils.getElementAlignment(config.getAlignment())).get());
			}
			
		}
		return table;
	}
	
	public static VerticalAlignment getVerticalAlignment(String alignment) {		
		
		if(!ValidationUtil.isHavingValue(alignment))
			return VerticalAlignment.TOP;
		
		alignment = alignment.toLowerCase();
		
		//return VerticalAlignment.valueOf(alignment);
		
		
		switch(alignment) {
			case PDFConfigValues.top:
				return VerticalAlignment.TOP;
			
			case PDFConfigValues.bottom:
				return VerticalAlignment.BOTTOM;
			
			case PDFConfigValues.center:
				return VerticalAlignment.CENTER;
			
			default:
				return VerticalAlignment.TOP;
		}
		
	}
	
	public static float[] getColumnWidthRatio(THeader tableHeaders, int colCount, String isTranspose) {
		float[] colWidthratio=null;
		if(Objects.nonNull(tableHeaders) && Objects.nonNull(tableHeaders.getColumns()) && tableHeaders.getColumns().length>0) {
			colWidthratio=new float[tableHeaders.getColumns().length];
			if(isTranspose.equals(PDFConfigValues.tableTransposeN)) {
				for(var colNo=0;colNo<tableHeaders.getColumns().length;colNo++) {
					if(Objects.nonNull(tableHeaders.getColumns()[colNo].getConfig())) {
						colWidthratio[colNo]=tableHeaders.getColumns()[colNo].getConfig().getWidth();
					}else {
						colWidthratio=null;
						break;
					}
				}
			}else {
				float headerSize=tableHeaders.getColumns()[0].getConfig().getWidth();
				if(headerSize<100) {
					float colSize=(float)(100-headerSize)/(colCount-1);
					colWidthratio=new float[colCount];
					colWidthratio[0]=headerSize;
					
					for(int i=1;i<colCount;i++) {
						colWidthratio[i]=colSize;
					}
				}else {
					colWidthratio=null;
				}
				
//				colWidthratio[0]=header
			}
		}
		return colWidthratio;
	}

}
