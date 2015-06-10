package com.ctrip.hermes.portal.view;

import com.ctrip.hermes.portal.console.ConsolePage;
import org.unidal.web.mvc.Page;

public class NavigationBar {
	public Page[] getVisiblePages() {
		return new Page[] {

		ConsolePage.TOPIC, ConsolePage.CONSUMER, ConsolePage.STORAGE, ConsolePage.ENDPOINT, ConsolePage.DASHBOARD };

	}
}
