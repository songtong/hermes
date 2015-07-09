package com.ctrip.hermes.portal.console.topic;

import java.util.List;

import org.unidal.web.mvc.ViewModel;

import com.ctrip.hermes.portal.console.ConsolePage;

public class Model extends ViewModel<ConsolePage, Action, Context> {

	private String m_topicName;

	private String m_kibanaUrl;

	private List<String> m_consumers;

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

	public String getKibanaUrl() {
		return m_kibanaUrl;
	}

	public void setKibanaUrl(String kibanaHost) {
		m_kibanaUrl = kibanaHost;
	}

	public List<String> getConsumers() {
		return m_consumers;
	}

	public void setConsumers(List<String> consumers) {
		m_consumers = consumers;
	}
}
