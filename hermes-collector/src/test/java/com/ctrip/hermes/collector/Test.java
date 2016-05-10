package com.ctrip.hermes.collector;

import java.io.IOException;
import java.util.Properties;

public class Test {
	public static void main(String args[]) throws IOException {
		Properties properties = new Properties();
		properties.load(Test.class.getResourceAsStream("/cb_mapping.properties"));
		System.out.println(properties);
	}
}
