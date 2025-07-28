package com.adp.esi.digitech.file.processing.config;

import java.util.concurrent.Executor;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
public class AppConfiguration {
	
	@Value("${app.daemon-tasks.thread-core-pool-size:20}")
	private int daemonTasksThreadCorePoolSize;
	
	@Value("${app.daemon-tasks.thread-max-pool-size:50}")
	private int daemonTasksThreadMaxPoolSize;
	
	@Value("${app.daemon-tasks.thread-queue-capacity:500}")
	private int daemonTasksThreadQueueCapacity;
	
	@Bean
	@Primary
	public Executor asyncExecutor () {
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setBeanName("asyncExecutor");
		executor.setCorePoolSize(daemonTasksThreadCorePoolSize);
		executor.setMaxPoolSize(daemonTasksThreadMaxPoolSize);
	    executor.setQueueCapacity(daemonTasksThreadQueueCapacity);
	    executor.setThreadNamePrefix("FPSThread-");
	    executor.initialize();
		return executor;
	}
}
