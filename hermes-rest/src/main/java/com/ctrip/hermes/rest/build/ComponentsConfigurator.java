package com.ctrip.hermes.rest.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.rest.service.DefaultSubscribeRegistry;
import com.ctrip.hermes.rest.service.MessagePushService;

public class ComponentsConfigurator extends AbstractResourceConfigurator {

	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		all.add(A(DefaultSubscribeRegistry.class));
		all.add(A(MessagePushService.class));

		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
