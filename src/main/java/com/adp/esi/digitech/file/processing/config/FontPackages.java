package com.adp.esi.digitech.file.processing.config;

import org.springframework.context.annotation.Configuration;

import com.lowagie.text.FontFactory;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;

@Configuration
@Slf4j
public class FontPackages {
	
	@PostConstruct
	private void loadFonts() {
		 FontFactory.registerDirectory("src/main/resources/fonts");
		 log.info("Fonts loaded");
	}

}
