package com.ctrip.hermes.portal.resource.view;

import java.util.List;

import org.unidal.tuple.Pair;

public class MonitorClientView {
	private String ip;

	private List<String> produceTopics;

	private List<Pair<String, List<String>>> consumeTopics;

	public MonitorClientView() {
	}

	public MonitorClientView(String ip) {
		this.ip = ip;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public List<String> getProduceTopics() {
		return produceTopics;
	}

	public void setProduceTopics(List<String> produceTopics) {
		this.produceTopics = produceTopics;
	}

	public List<Pair<String, List<String>>> getConsumeTopics() {
		return consumeTopics;
	}

	public void setConsumeTopics(List<Pair<String, List<String>>> consumeTopics) {
		this.consumeTopics = consumeTopics;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((ip == null) ? 0 : ip.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MonitorClientView other = (MonitorClientView) obj;
		if (ip == null) {
			if (other.ip != null)
				return false;
		} else if (!ip.equals(other.ip))
			return false;
		return true;
	}
}
