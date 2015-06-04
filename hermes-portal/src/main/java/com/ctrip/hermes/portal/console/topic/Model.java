package com.ctrip.hermes.portal.console.topic;

import com.ctrip.hermes.portal.console.ConsolePage;
import org.unidal.web.mvc.ViewModel;

public class Model extends ViewModel<ConsolePage, Action, Context> {

	private String m_topicName;

	public Model(Context ctx) {
		super(ctx);
	}

	@Override
	public Action getDefaultAction() {
		return Action.VIEW;
	}

	public String getTopicName() {
		return m_topicName;
	}

	public void setTopicName(String topicName) {
		m_topicName = topicName;
	}
}
