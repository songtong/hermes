package com.ctrip.hermes.monitor.checker;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class BaseCheckerTest {
	protected String loadTestData(String methodName) throws IOException, URISyntaxException {
		return new String(Files.readAllBytes(Paths.get(this.getClass()
		      .getResource(this.getClass().getSimpleName() + "-" + methodName + ".xml").toURI())));
	}
}
