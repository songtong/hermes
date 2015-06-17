package com.ctrip.hermes.metaserver.meta.watcher;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.Test;

public class ZkWatcherTest {

	@SuppressWarnings("resource")
	@Test
	public void test() throws Exception {
		new TestingServer(2181).start();

		Builder builder = CuratorFrameworkFactory.builder();
		builder.retryPolicy(new ExponentialBackoffRetry(100, 1));
		builder.connectString("127.0.0.1:2181");
		CuratorFramework client = builder.build();
		client.start();

		final CountDownLatch latch = new CountDownLatch(2);

		client.getChildren().usingWatcher(new Watcher() {

			@Override
			public void process(WatchedEvent event) {
				latch.countDown();
			}

		}).forPath("/");

		client.getChildren().usingWatcher(new Watcher() {

			@Override
			public void process(WatchedEvent event) {
				latch.countDown();
			}

		}).forPath("/");

		client.create().forPath("/a");

		assertTrue(latch.await(100, TimeUnit.MILLISECONDS));
	}

}
