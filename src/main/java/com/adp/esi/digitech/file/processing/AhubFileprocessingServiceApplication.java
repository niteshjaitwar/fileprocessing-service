package com.adp.esi.digitech.file.processing;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@EnableAsync
@EnableScheduling
@Slf4j
public class AhubFileprocessingServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(AhubFileprocessingServiceApplication.class, args);
		log.info("Application started successfully, processors = {}", Runtime.getRuntime().availableProcessors());
	}

}
