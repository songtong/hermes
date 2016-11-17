package com.ctrip.hermes.admin.core.service.template;

import java.io.StringWriter;
import java.io.Writer;
import java.util.Map;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Named;

import freemarker.cache.ClassTemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;

@Named(type = TemplateService.class)
public class DefaultTemplateService implements TemplateService, Initializable {
	private static final Logger log = LoggerFactory.getLogger(DefaultTemplateService.class);

	private Configuration m_templateConfig;

	@Override
	public String render(HermesTemplate hermesTemplate, Map<String, Object> contentMap) {
		try {
			Template template = m_templateConfig.getTemplate(hermesTemplate.getName());
			if (template != null) {
				Writer writer = new StringWriter();
				template.process(contentMap, writer);
				return writer.toString();
			} else {
				log.error("Can not find template {}", hermesTemplate.getName());
			}
		} catch (Exception e) {
			log.error("Process template failed, template name:[{}], content map:{}", hermesTemplate.getName(), contentMap,
			      e);
		}
		return "";
	}

	@Override
	public void initialize() throws InitializationException {
		m_templateConfig = new Configuration(Configuration.VERSION_2_3_23);
		m_templateConfig.setDefaultEncoding("UTF-8");
		m_templateConfig.setTemplateLoader(new ClassTemplateLoader(getClass(), "/templates"));
		m_templateConfig.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
	}
}
