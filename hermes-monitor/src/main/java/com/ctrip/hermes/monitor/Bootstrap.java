package com.ctrip.hermes.monitor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.web.SpringBootServletInitializer;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@SpringBootApplication
@EnableScheduling
public class Bootstrap extends SpringBootServletInitializer {

	@Override
	protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
		return application.sources(Bootstrap.class);
	}

	public static void main(String[] args) {
		SpringApplication.run(Bootstrap.class);
	}

}
