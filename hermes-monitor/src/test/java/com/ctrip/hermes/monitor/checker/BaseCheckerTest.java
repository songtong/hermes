package com.ctrip.hermes.monitor.checker;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@SpringBootApplication
@ComponentScan(basePackages = "com.ctrip.hermes.monitor")
public class BaseCheckerTest {
	protected String loadTestData(String methodName) throws IOException, URISyntaxException {
		return new String(Files.readAllBytes(Paths.get(this.getClass()
		      .getResource(this.getClass().getSimpleName() + "-" + methodName + ".xml").toURI())));
	}

	public static String loadTestData(String methodName, Class<?> clazz) throws IOException, URISyntaxException {
		return new String(Files.readAllBytes(Paths.get(clazz.getResource(
		      clazz.getSimpleName() + "-" + methodName + ".xml").toURI())));
	}

	public static void main(String[] args) {
		SpringApplication.run(BaseCheckerTest.class);
	}
}
