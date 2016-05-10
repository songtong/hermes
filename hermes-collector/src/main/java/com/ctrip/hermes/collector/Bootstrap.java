package com.ctrip.hermes.collector;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.web.SpringBootServletInitializer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * @author tenglinxiao
 *
 */
@SpringBootApplication
@EnableScheduling
@EnableAsync
public class Bootstrap extends SpringBootServletInitializer {

	@Override
	protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
		return application.sources(Bootstrap.class);
	}

	public static void main(String[] args) {
		SpringApplication.run(Bootstrap.class);
	}
}
