package com.ctrip.hermes.portal.view;

import org.unidal.web.mvc.Page;

import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.portal.console.ConsolePage;

public class NavigationBar {
	public Page[] getVisiblePages() {
		return new Page[] {

		ConsolePage.TOPIC, ConsolePage.CONSUMER, ConsolePage.SUBSCRIPTION, ConsolePage.STORAGE, ConsolePage.ENDPOINT,
		      ConsolePage.DASHBOARD, ConsolePage.TRACER, ConsolePage.RESENDER };

	}

	public String getEnvironment(){
		return PlexusComponentLocator.lookup(ClientEnvironment.class).getEnv().name();
	}
}
