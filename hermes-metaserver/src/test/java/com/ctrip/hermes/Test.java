package com.ctrip.hermes;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.retry.RetryNTimes;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;

public class Test {

	public static void main(String[] args) throws Exception {

		Builder builder = CuratorFrameworkFactory.builder();

		builder.connectionTimeoutMs(1000);
		builder.connectString("127.0.0.1:2181");
		builder.retryPolicy(new RetryNTimes(1, 1000));
		builder.sessionTimeoutMs(5000);

		CuratorFramework framework = builder.build();
		framework.start();
		try {
			framework.blockUntilConnected();
		} catch (InterruptedException e) {
			throw new InitializationException(e.getMessage(), e);
		}

		framework.createContainers("/hermes1/b");
		System.in.read();
		// framework.checkExists().creatingParentContainersIfNeeded().forPath("/hermes/b");
		framework.createContainers("/hermes1/b");
		System.in.read();
		framework.createContainers("/hermes1/b");
		System.in.read();
	}

}
