package com.ctrip.hermes.monitor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@SpringBootApplication
@EnableScheduling
public class Bootstrap {

	public static void main(String[] args) {
		SpringApplication.run(Bootstrap.class);
	}

}
