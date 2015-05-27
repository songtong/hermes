package com.ctrip.hermes.portal.topic.home;

import com.ctrip.hermes.portal.topic.TopicPage;
import org.unidal.web.mvc.ViewModel;

public class Model extends ViewModel<TopicPage, Action, Context> {
	public Model(Context ctx) {
		super(ctx);
	}

	@Override
	public Action getDefaultAction() {
		return Action.VIEW;
	}
}
