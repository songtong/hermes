package com.ctrip.hermes.admin.core.service.template;

import java.util.Map;

public interface TemplateService {
	public String render(HermesTemplate type, Map<String, Object> contentMap);
}
