package com.ctrip.hermes.metaserver.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;

import com.ctrip.hermes.core.env.ClientEnvironment;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class ZKClient implements Initializable {
	private static final String CONFIG_PREFIX = "metaserver.zk.";

	private CuratorFramework m_client;

	private ClientEnvironment m_env;

	@Override
	public void initialize() throws InitializationException {
		Builder builder = CuratorFrameworkFactory.builder();
		builder.connectionTimeoutMs(Integer.valueOf(m_env.getGlobalConfig().getProperty(
		      CONFIG_PREFIX + "connection.timeout")));
		builder.connectString(m_env.getGlobalConfig().getProperty(CONFIG_PREFIX + "connection.url"));
//		builder.maxCloseWaitMs(m_env.getGlobalConfig().getProperty(CONFIG_PREFIX + "close.wait"));
	}
}
