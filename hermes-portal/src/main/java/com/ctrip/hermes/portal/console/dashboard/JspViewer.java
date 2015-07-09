package com.ctrip.hermes.portal.console.dashboard;

import com.ctrip.hermes.portal.console.ConsolePage;
import org.unidal.web.mvc.view.BaseJspViewer;

public class JspViewer extends BaseJspViewer<ConsolePage, Action, Context, Model> {
	@Override
	protected String getJspFilePath(Context ctx, Model model) {
		Action action = model.getAction();

		switch (action) {
		case TOPIC:
			return JspFile.TOPIC.getPath();
		case TOPIC_DETAIL:
			return JspFile.TOPIC_DETAIL.getPath();
		case BROKER:
			return JspFile.BROKER.getPath();
		case BROKER_DETAIL:
			return JspFile.BROKER_DETAIL.getPath();
		case BROKER_DETAIL_HOME:
			return JspFile.BROKER_DETAIL_HOME.getPath();
		case CLIENT:
			return JspFile.CLIENT.getPath();
		case CLIENT_DETAIL:
			return JspFile.CLIENT_DETAIL.getPath();
		}

		throw new RuntimeException("Unknown action: " + action);
	}
}
