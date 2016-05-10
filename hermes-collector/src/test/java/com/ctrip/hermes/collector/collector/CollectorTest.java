package com.ctrip.hermes.collector.collector;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = "com.ctrip.hermes.collector")
public class CollectorTest {
	protected String loadTestData(String methodName) throws IOException, URISyntaxException {
		return new String(Files.readAllBytes(Paths.get(this.getClass()
		      .getResource(this.getClass().getSimpleName() + "-" + methodName + ".xml").toURI())));
	}

}
