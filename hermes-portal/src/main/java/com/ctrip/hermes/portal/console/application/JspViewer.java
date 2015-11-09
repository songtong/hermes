package com.ctrip.hermes.portal.console.application;

import com.ctrip.hermes.portal.console.ConsolePage;
import org.unidal.web.mvc.view.BaseJspViewer;

public class JspViewer extends BaseJspViewer<ConsolePage, Action, Context, Model> {
	@Override
	protected String getJspFilePath(Context ctx, Model model) {
		Action action = model.getAction();

		switch (action) {
		case VIEW:
			return JspFile.VIEW.getPath();
		case TOPIC:
			return JspFile.TOPIC.getPath();
		case CONSUMER:
			return JspFile.CONSUMER.getPath();
		case REVIEW:
			return JspFile.REVIEW.getPath();
		default:
			break;
		}

		throw new RuntimeException("Unknown action: " + action);
	}
}
