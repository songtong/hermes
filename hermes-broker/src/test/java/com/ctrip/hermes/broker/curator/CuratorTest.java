package com.ctrip.hermes.broker.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;
import org.unidal.net.Networks;

public class CuratorTest {

	@Test
	public void test() throws Exception {
		TestingServer zk = new TestingServer(2181);
		zk.start();

		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework client = CuratorFrameworkFactory.builder().namespace("hermes").connectString("127.0.0.1:2181")
		      .retryPolicy(retryPolicy).sessionTimeoutMs(5000).build();
		client.start();

		String ip = Networks.forIp().getLocalHostAddress();
		String path = String.format("%s%s", "/brokers/", ip);
		try {
			client.delete().forPath(path);
		} catch (Exception e) {
		}
		client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path);
		
		
		System.in.read();
		zk.close();
	}

}
