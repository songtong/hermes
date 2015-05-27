package com.ctrip.hermes.portal.build;

import java.util.ArrayList;
import java.util.List;

import com.ctrip.hermes.portal.topic.TopicModule;

import org.unidal.lookup.configuration.Component;
import org.unidal.web.configuration.AbstractWebComponentsConfigurator;

class WebComponentConfigurator extends AbstractWebComponentsConfigurator {
	@SuppressWarnings("unchecked")
	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		defineModuleRegistry(all, TopicModule.class, TopicModule.class);

		return all;
	}
}
