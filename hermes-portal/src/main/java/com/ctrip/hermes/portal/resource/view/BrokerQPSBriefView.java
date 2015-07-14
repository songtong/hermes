package com.ctrip.hermes.portal.resource.view;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class BrokerQPSBriefView {
	private String brokerIp;

	private int qps;

	public BrokerQPSBriefView() {

	}

	public BrokerQPSBriefView(String brokerIp, int qps) {
		this.brokerIp = brokerIp;
		this.qps = qps;
	}

	public static List<BrokerQPSBriefView> convertFromMap(Map<String, Integer> map) {
		List<BrokerQPSBriefView> list = new ArrayList<BrokerQPSBriefView>();
		if (map != null) {
			for (Entry<String, Integer> entry : map.entrySet()) {
				list.add(new BrokerQPSBriefView(entry.getKey(), entry.getValue()));
			}
		}
		return list;
	}

	public String getBrokerIp() {
		return brokerIp;
	}

	public void setBrokerIp(String brokerIp) {
		this.brokerIp = brokerIp;
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
		result = prime * result + ((brokerIp == null) ? 0 : brokerIp.hashCode());
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
		BrokerQPSBriefView other = (BrokerQPSBriefView) obj;
		if (brokerIp == null) {
			if (other.brokerIp != null)
				return false;
		} else if (!brokerIp.equals(other.brokerIp))
			return false;
		return true;
	}
}
