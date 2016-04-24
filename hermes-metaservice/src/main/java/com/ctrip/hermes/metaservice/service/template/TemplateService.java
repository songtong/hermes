package com.ctrip.hermes.metaservice.service.template;

import java.util.Map;

public interface TemplateService {
	public String render(HermesTemplate type, Map<String, Object> contentMap);
}
