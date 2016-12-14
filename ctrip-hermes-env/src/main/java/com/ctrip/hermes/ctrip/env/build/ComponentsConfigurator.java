package com.ctrip.hermes.ctrip.env.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.ctrip.env.CtripEnvProvider;
import com.ctrip.hermes.ctrip.env.CtripManualConfigProvider;
import com.ctrip.hermes.ctrip.env.DefaultClientEnvironment;
import com.ctrip.hermes.ctrip.env.config.broker.CtripBrokerConfigProvider;

public class ComponentsConfigurator extends AbstractResourceConfigurator {

	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		all.add(A(DefaultClientEnvironment.class));
		all.add(A(CtripEnvProvider.class));
		all.add(A(CtripManualConfigProvider.class));
		all.add(A(CtripBrokerConfigProvider.class));

		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
