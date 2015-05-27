package com.ctrip.hermes.portal.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.portal.service.storage.TopicStorageService;
import com.ctrip.hermes.portal.service.storage.handler.MysqlStorageHandler;

public class ComponentsConfigurator extends AbstractResourceConfigurator {
	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();
		// storage handler
		all.add(A(TopicStorageService.class));
		all.add(A(MysqlStorageHandler.class));


		// Please keep it as last
		all.addAll(new WebComponentConfigurator().defineComponents());

		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
