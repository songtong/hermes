package com.ctrip.hermes.metaserver.commons;

import java.util.Map;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.bo.HostPort;
import com.ctrip.hermes.core.bo.Tpg;

public class MetaStatusStatusResponse {
	private Boolean leader;

	private HostPort leaderInfo;

	private Map<String, Assignment<Integer>> brokerAssignments;

	private Map<Pair<String, Integer>, Map<String, ClientLeaseInfo>> brokerLeases;

	private Map<Tpg, Map<String, ClientLeaseInfo>> consumerLeases;

	private Assignment<String> metaServerAssignments;

	private Map<Pair<String, String>, Assignment<Integer>> consumerAssignments;

	public Boolean isLeader() {
		return leader;
	}

	public void setLeader(Boolean leader) {
		this.leader = leader;
	}

	public Map<String, Assignment<Integer>> getBrokerAssignments() {
		return brokerAssignments;
	}

	public void setBrokerAssignments(Map<String, Assignment<Integer>> brokerAssignments) {
		this.brokerAssignments = brokerAssignments;
	}

	public Map<Pair<String, Integer>, Map<String, ClientLeaseInfo>> getBrokerLeases() {
		return brokerLeases;
	}

	public void setBrokerLeases(Map<Pair<String, Integer>, Map<String, ClientLeaseInfo>> brokerLeases) {
		this.brokerLeases = brokerLeases;
	}

	public Map<Tpg, Map<String, ClientLeaseInfo>> getConsumerLeases() {
		return consumerLeases;
	}

	public void setConsumerLeases(Map<Tpg, Map<String, ClientLeaseInfo>> consumerLeases) {
		this.consumerLeases = consumerLeases;
	}

	public HostPort getLeaderInfo() {
		return leaderInfo;
	}

	public void setLeaderInfo(HostPort leaderInfo) {
		this.leaderInfo = leaderInfo;
	}

	public Assignment<String> getMetaServerAssignments() {
		return metaServerAssignments;
	}

	public void setMetaServerAssignments(Assignment<String> metaServerAssignments) {
		this.metaServerAssignments = metaServerAssignments;
	}

	public void setConsumerAssignments(Map<Pair<String, String>, Assignment<Integer>> consumerAssignments) {
		this.consumerAssignments = consumerAssignments;
	}

	public Map<Pair<String, String>, Assignment<Integer>> getConsumerAssignments() {
		return consumerAssignments;
	}
}