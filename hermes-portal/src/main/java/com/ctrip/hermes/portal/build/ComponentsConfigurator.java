package com.ctrip.hermes.portal.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.portal.service.CodecService;
import com.ctrip.hermes.portal.service.CompileService;
import com.ctrip.hermes.portal.service.DefaultMetaServiceWrapper;
import com.ctrip.hermes.portal.service.SchemaService;
import com.ctrip.hermes.portal.service.TopicService;
import com.ctrip.hermes.portal.service.storage.DefaultTopicStorageService;
import com.ctrip.hermes.portal.service.storage.handler.MysqlStorageHandler;

public class ComponentsConfigurator extends AbstractResourceConfigurator {

	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		// storage handler
		all.add(A(DefaultTopicStorageService.class));
		all.add(A(MysqlStorageHandler.class));

		all.add(A(TopicService.class));
		all.add(A(SchemaService.class));
		all.add(A(CodecService.class));
		all.add(A(CompileService.class));

		all.add(A(DefaultMetaServiceWrapper.class));

		// Please keep it as last
		all.addAll(new WebComponentConfigurator().defineComponents());

		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
