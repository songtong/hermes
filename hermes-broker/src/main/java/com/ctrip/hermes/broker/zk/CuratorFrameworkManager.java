package com.ctrip.hermes.broker.zk;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Named;

@Named
public class CuratorFrameworkManager implements Initializable {

	private CuratorFramework m_client;

	public CuratorFramework getClient() {
		return m_client;
	}

	@Override
	public void initialize() throws InitializationException {
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(500, 3);
		// TODO config
		m_client = CuratorFrameworkFactory.builder().namespace("hermes").connectString("127.0.0.1:2181")
		      .retryPolicy(retryPolicy).sessionTimeoutMs(5000).build();
		m_client.start();
	}

}
