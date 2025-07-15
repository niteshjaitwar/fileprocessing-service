package com.adp.esi.digitech.file.processing.model;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Metadata {
	
	List<String> reqheaders;
	List<String> dbHeaders;

}
