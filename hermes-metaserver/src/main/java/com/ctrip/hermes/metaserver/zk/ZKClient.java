package com.ctrip.hermes.metaserver.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;

import com.ctrip.hermes.metaserver.config.MetaServerConfig;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class ZKClient implements Initializable {
	private static final String CONFIG_PREFIX = "metaserver.zk.";

	private CuratorFramework m_client;

	private MetaServerConfig m_config;

	@Override
	public void initialize() throws InitializationException {
		Builder builder = CuratorFrameworkFactory.builder();

		builder.connectionTimeoutMs(m_config.getZkConnectionTimeoutMillis());
		builder.connectString(m_config.getZkConnectionString());
		builder.maxCloseWaitMs(m_config.getZkCloseWaitMillis());
		builder.namespace(m_config.getZkNamespace());

		m_client = builder.build();
	}

	public CuratorFramework getClient() {
		return m_client;
	}
}
