package com.ctrip.hermes.portal.mail;

import java.io.File;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;

public class FreemarkerTest {
	public class Product {
		private String url = "http://127.0.0.1/index.jsp";

		private String name = "index_page";

		public String getUrl() {
			return url;
		}

		public String getName() {
			return name;
		}
	}

	@Test
	public void testFreemarker() throws Exception {
		Map<String, Object> m = new HashMap<>();
		m.put("user", "song.tong");
		Product p = new Product();
		m.put("p", p);

		Configuration cfg = new Configuration(Configuration.VERSION_2_3_23);
		File dir = new File(getClass().getResource("/ftls").toURI());
		cfg.setDirectoryForTemplateLoading(dir);
		cfg.setDefaultEncoding("UTF-8");
		cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
		Template template = cfg.getTemplate("test.ftl");
		Writer out = new OutputStreamWriter(System.out);
		template.process(m, out);

	}
}
