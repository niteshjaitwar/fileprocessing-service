package com.adp.esi.digitech.file.processing.ds.config.model;

import java.util.List;

import com.adp.esi.digitech.file.processing.enums.RelationshipType;

import lombok.Data;

@Data
public class Reference {
	private String sourceKey;
	private String type;
	private int headerIndex;
	private RelationshipType relationship;
	private int dataRowNumber;
	private List<KeyIdentifier> keyIdentifiers;
}
