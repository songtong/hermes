package com.ctrip.hermes.portal.topic.home;

import com.ctrip.hermes.portal.topic.TopicPage;
import org.unidal.web.mvc.view.BaseJspViewer;

public class JspViewer extends BaseJspViewer<TopicPage, Action, Context, Model> {
	@Override
	protected String getJspFilePath(Context ctx, Model model) {
		Action action = model.getAction();

		switch (action) {
		case VIEW:
			return JspFile.VIEW.getPath();
		}

		throw new RuntimeException("Unknown action: " + action);
	}
}
