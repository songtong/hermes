package com.ctrip.hermes.rest.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.rest.service.CmessageTransferService;
import com.ctrip.hermes.rest.service.HttpPushService;
import com.ctrip.hermes.rest.service.ProducerSendService;
import com.ctrip.hermes.rest.service.SoaPushService;
import com.ctrip.hermes.rest.service.SubscriptionRegisterService;

public class ComponentsConfigurator extends AbstractResourceConfigurator {

	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		all.add(A(SubscriptionRegisterService.class));
		all.add(A(HttpPushService.class));
		all.add(A(SoaPushService.class));
		all.add(A(ProducerSendService.class));
		all.add(A(CmessageTransferService.class));

		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
