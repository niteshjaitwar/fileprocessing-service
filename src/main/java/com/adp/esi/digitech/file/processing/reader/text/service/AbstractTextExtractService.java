package com.adp.esi.digitech.file.processing.reader.text.service;

import java.util.function.Function;

public abstract class AbstractTextExtractService {
	
	protected Function<String, String> uuidFun = (input) -> input.substring(2, input.length() - 2);
}
