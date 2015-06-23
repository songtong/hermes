package com.ctrip.hermes.metaserver.cluster;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.bo.HostPort;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ClusterTopicAssignmentHolder.class)
public class ClusterTopicAssignmentHolder {

	@Inject
	private MetaServerConfig m_config;

	private AtomicReference<Map<String, String>> m_topic2MetaServerName = new AtomicReference<Map<String, String>>(
	      new HashMap<String, String>());

	public TopicAssignmentResult findAssignment(String topic) {
		Map<String, String> topic2MetaServerName = m_topic2MetaServerName.get();
		if (topic2MetaServerName == null) {
			return null;
		}
		String responsor = topic2MetaServerName.get(topic);
		if (StringUtils.isBlank(responsor)) {
			return null;
		}

		if (m_config.getMetaServerName().equals(responsor)) {
			return new TopicAssignmentResult(true, null, -1);
		} else {
			HostPort hostPort = JSON.parseObject(responsor, HostPort.class);
			return new TopicAssignmentResult(false, hostPort.getHost(), hostPort.getPort());
		}
	}

	public static class TopicAssignmentResult {
		private boolean m_assignToMe;

		private String m_responsorHost;

		private int m_responsorPort;

		public TopicAssignmentResult(boolean assignToMe, String responsorHost, int responsorPort) {
			m_assignToMe = assignToMe;
			m_responsorHost = responsorHost;
			m_responsorPort = responsorPort;
		}

		public boolean isAssignToMe() {
			return m_assignToMe;
		}

		public String getResponsorHost() {
			return m_responsorHost;
		}

		public int getResponsorPort() {
			return m_responsorPort;
		}

	}
}
