package com.ctrip.hermes.portal.view;

import org.unidal.web.mvc.Page;

import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.portal.console.ConsolePage;

public class NavigationBar {
	private Page[] BASE_PAGES = new Page[] { ConsolePage.DASHBOARD, ConsolePage.TRACER, ConsolePage.TOPIC,
			ConsolePage.APPLICATION };
	private Page[] ALL_PAGES = new Page[] { ConsolePage.DASHBOARD, ConsolePage.TOPIC, ConsolePage.CONSUMER,
			ConsolePage.SUBSCRIPTION, ConsolePage.STORAGE, ConsolePage.ENDPOINT, ConsolePage.TRACER,
			ConsolePage.RESENDER, ConsolePage.APPLICATION };

	public Page[] getBasePages() {
		return BASE_PAGES;
	}

	public Page[] getAllPages() {
		return ALL_PAGES;
	}

	public String getEnvironment() {
		return PlexusComponentLocator.lookup(ClientEnvironment.class).getEnv().name();
	}
}
