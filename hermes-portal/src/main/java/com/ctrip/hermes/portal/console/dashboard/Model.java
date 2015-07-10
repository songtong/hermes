package com.ctrip.hermes.portal.console.dashboard;

import com.ctrip.hermes.portal.console.ConsolePage;
import org.unidal.web.mvc.ViewModel;

public class Model extends ViewModel<ConsolePage, Action, Context> {
	private String topic;

	private String consumer;

	private String kibanaUrl;

	private String brokerIP;

	private String clientIP;

	public String getBrokerIP() {
		return brokerIP;
	}

	public void setBrokerIP(String brokerIP) {
		this.brokerIP = brokerIP;
	}

	public String getClientIP() {
		return clientIP;
	}

	public void setClientIP(String clientIP) {
		this.clientIP = clientIP;
	}

	public Model(Context ctx) {
		super(ctx);
	}

	@Override
	public Action getDefaultAction() {
		return Action.TOPIC;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getConsumer() {
		return consumer;
	}

	public void setConsumer(String consumer) {
		this.consumer = consumer;
	}

	public String getKibanaUrl() {
		return kibanaUrl;
	}

	public void setKibanaUrl(String kibanaUrl) {
		this.kibanaUrl = kibanaUrl;
	}
}
