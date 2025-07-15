package com.adp.esi.digitech.file.processing.generator.document.pdf.element.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.adp.esi.digitech.file.processing.generator.document.config.model.ElmTable;
import com.adp.esi.digitech.file.processing.generator.document.config.model.TBody;
import com.adp.esi.digitech.file.processing.generator.document.config.model.TConfig;
import com.adp.esi.digitech.file.processing.generator.document.config.model.THeader;
import com.adp.esi.digitech.file.processing.generator.document.config.model.TRow;
import com.adp.esi.digitech.file.processing.exception.GenerationException;
import com.adp.esi.digitech.file.processing.generator.document.config.model.Element;
import com.adp.esi.digitech.file.processing.generator.document.element.IElementBuilder;
import com.adp.esi.digitech.file.processing.generator.document.enums.TransposeType;
import com.adp.esi.digitech.file.processing.generator.pdf.util.ChunkBuilder;
import com.adp.esi.digitech.file.processing.generator.pdf.util.GenericUtils;
import com.adp.esi.digitech.file.processing.generator.pdf.util.PDFConfigValues;
import com.adp.esi.digitech.file.processing.generator.pdf.util.ParagraphUtils;
import com.adp.esi.digitech.file.processing.generator.pdf.util.TableUtils;
import com.adp.esi.digitech.file.processing.model.DataSet;
import com.adp.esi.digitech.file.processing.model.Row;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lowagie.text.Cell;
import com.lowagie.text.Document;
import com.lowagie.text.Paragraph;
import com.lowagie.text.Table;

@Component("pdftable")
public class PDFTableBuilder implements IElementBuilder<Document>{

	@Autowired
	ObjectMapper mapper;
	
	@Override
	public void build(Document doc, Element element, DataSet<Row> data) {
		var table = (ElmTable) element.getComponent();
		createTables(doc,table, data);
	}
	
	@Override
	public com.lowagie.text.Element getElement(Element element, DataSet<Row> data) {
		var tableElement = (ElmTable) element.getComponent();
		Table table=TableUtils.getTable(TableUtils.joinHeaderANDBody(getHeaderRow(tableElement.getHeader()), splitBodyWithMaxRowsAndTranspose(getBodyDataRows(tableElement.getBody(), data),0,tableElement.getConfig().getIsTableTranspose()).get(0), tableElement.getConfig().getIsTableTranspose()),tableElement.getConfig(),TableUtils.getColumnWidthRatio(tableElement.getHeader(),0,PDFConfigValues.tableTransposeN));
		return table;
	}
	
	private Cell[] getHeaderRow(THeader header) {
		Cell[] cell=null;
		if(Objects.nonNull(header)) {
			cell=new Cell[header.getColumns().length];
			for(var i=0;i<header.getColumns().length;i++){
				cell[i]=TableUtils.createCell(ChunkBuilder.create(header.getColumns()[i].getLabel(),header.getConfig().getFont()),header.getConfig().getWidth(),true,header.getConfig().getBgColor(),header.getConfig().getColspan());
			}
		}
		return cell;
	}
	
	private Cell[][] getDynamicDataRows(String[] columns, DataSet<Row> data,TConfig config) {
		Cell[][] tableCells=null;
//		ValidationUtil.
		if(GenericUtils.isValidArray(columns) && Objects.nonNull(data) && GenericUtils.isValidList(data.getData())) {
			tableCells=new Cell[data.getData().size()][columns.length];
			for(var rowNo=0;rowNo<data.getData().size();rowNo++) {
				for(var columnNo=0; columnNo<columns.length;columnNo++) {
					var targetValue="";
					if(Objects.nonNull(data.getData().get(rowNo).getColumns().get(UUID.fromString(columns[columnNo])))) {
						targetValue=data.getData().get(rowNo).getColumns().get(UUID.fromString(columns[columnNo])).getTargetValue();
					}
					tableCells[rowNo][columnNo]=TableUtils.createCell( ChunkBuilder.create( targetValue,config.getFont()),config.getWidth(), false,config.getBgColor(),config.getColspan());
				}
			}
		}
		return tableCells;
	}
	
	private Cell[][] getStaticDataRows(TRow[] rows, TConfig config, DataSet<Row> data) {
		rows=replaceDynamicFields(rows,data);
		Cell[][] tableCells=null;

		if(GenericUtils.isValidArray(rows)) {
			tableCells=new Cell[rows.length][rows[0].getColumns().length];
			for(var rowNo=0;rowNo<rows.length;rowNo++) {
				for(var columnNo=0;columnNo<rows[0].getColumns().length;columnNo++) {
					tableCells[rowNo][columnNo]=TableUtils.createCell( ChunkBuilder.create( rows[rowNo].getColumns()[columnNo],config.getFont()),config.getWidth(), false,config.getBgColor(),config.getColspan());
				}
			}
		}
		return tableCells;
	}
	
	private Cell[][] getBodyDataRows(TBody body, DataSet<Row> data) {
		Cell[][] bodyRows=null;
		if(Objects.nonNull(Objects.nonNull(body))) {
			if(body.getContentType().equals(PDFConfigValues.tableBodyTypeDynamic)) {
				bodyRows=getDynamicDataRows(body.getDynamicRows().getColumns(),data,body.getConfig());
			}else {
				bodyRows=getStaticDataRows(body.getStaticRows(), body.getConfig(),data);
			}
		}
		return bodyRows;
	}
	
	private List<Cell[][]> splitBodyWithMaxRowsAndTranspose(Cell[][] matrix,int maxRows, String isTranspose) {
		List<Cell[][]> retSplittedRows=new ArrayList<Cell[][]>();
		if(Objects.nonNull(matrix)) {
			
			var splittedRows=TableUtils.splitMatrix(matrix, maxRows);
			if(isTranspose.equals(TransposeType.Yes.getTranposeType())) {
				splittedRows.forEach(splittedMatrix ->{
					retSplittedRows.add(TableUtils.transposeMatrix(splittedMatrix));
				});
				return retSplittedRows;
			}else {
				return splittedRows;
			}
		}else {
			return null;
		}
	}
	

	

	
	public void createTable(Document doc,Cell[][] cellMatrix,TConfig config, THeader header, String isTranspose) {
		doc.add(TableUtils.getTable(cellMatrix, config,TableUtils.getColumnWidthRatio(header,cellMatrix[0].length,isTranspose)));
//		doc.add(new Paragraph("\n"));
	}
	
	public void createTables(Document doc, ElmTable tableConfig, DataSet<Row> data) {
		List<Cell[][]> splittedMatrix=splitBodyWithMaxRowsAndTranspose(getBodyDataRows(tableConfig.getBody(), data),tableConfig.getConfig().getMaxRows(),tableConfig.getConfig().getIsTableTranspose());
		if(Objects.nonNull(splittedMatrix)) {
			splittedMatrix.forEach(table ->{
				if(Objects.nonNull(tableConfig)) {
					createTable(doc,TableUtils.joinHeaderANDBody(getHeaderRow(tableConfig.getHeader()),table,tableConfig.getConfig().getIsTableTranspose()),tableConfig.getConfig(),tableConfig.getHeader(),tableConfig.getConfig().getIsTableTranspose());
				}
			});
		}else {
			createTable(doc,TableUtils.joinHeaderANDBody(getHeaderRow(tableConfig.getHeader()),null,tableConfig.getConfig().getIsTableTranspose()),tableConfig.getConfig(),tableConfig.getHeader(),tableConfig.getConfig().getIsTableTranspose());
		}
	}
	

	private TRow[] replaceDynamicFields(TRow[] rows, DataSet<Row> data) {
		if(Objects.nonNull(rows) && rows.length>0) {
			try {
				String rowsString=mapper.writeValueAsString(rows);
				rowsString=ParagraphUtils.replaceFields(rowsString, data);
				rows=mapper.readValue(rowsString, TRow[].class);
			} catch (JsonProcessingException e) {
				
			}
		}
		return rows;
	}

}
