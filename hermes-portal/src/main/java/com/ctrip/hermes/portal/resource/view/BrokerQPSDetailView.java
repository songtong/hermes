package com.ctrip.hermes.portal.resource.view;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class BrokerQPSDetailView extends BrokerQPSBriefView {
	private List<BrokerQPSDetail> qpsDetail;

	public BrokerQPSDetailView() {

	}

	public BrokerQPSDetailView(String brokerIp, Map<String, Integer> qpsMap) {
		setBrokerIp(brokerIp);

		this.qpsDetail = new ArrayList<BrokerQPSDetail>();
		for (Entry<String, Integer> entry : qpsMap.entrySet()) {
			this.qpsDetail.add(new BrokerQPSDetail(entry.getKey(), entry.getValue()));
		}
	}

	public List<BrokerQPSDetail> getQpsDetail() {
		return qpsDetail;
	}

	public void setQpsDetail(List<BrokerQPSDetail> qpsDetail) {
		this.qpsDetail = qpsDetail;
	}

	public static class BrokerQPSDetail {
		private String topic;

		private int qps;

		public BrokerQPSDetail() {

		}

		public BrokerQPSDetail(String topic, int qps) {
			this.topic = topic;
			this.qps = qps;
		}

		public String getTopic() {
			return topic;
		}

		public void setTopic(String topic) {
			this.topic = topic;
		}

		public int getQps() {
			return qps;
		}

		public void setQps(int qps) {
			this.qps = qps;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((topic == null) ? 0 : topic.hashCode());
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
			BrokerQPSDetail other = (BrokerQPSDetail) obj;
			if (topic == null) {
				if (other.topic != null)
					return false;
			} else if (!topic.equals(other.topic))
				return false;
			return true;
		}
	}
}
