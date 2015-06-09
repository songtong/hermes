package com.ctrip.hermes.metaserver;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class LeaderTest2 {
	private static final int CLIENT_QTY = 2;

	private static final String PATH = "/examples/leader";

	private AtomicBoolean stopped = new AtomicBoolean(false);

	public static void main(String[] args) throws Exception {

		final String name = "Client-" + System.currentTimeMillis();

		CuratorFramework client = CuratorFrameworkFactory.newClient("127.0.0.1:2181",
		      new ExponentialBackoffRetry(1000, 3));
		client.start();
		final LeaderLatch latch = new LeaderLatch(client, PATH);

		latch.addListener(new LeaderLatchListener() {

			@Override
			public void notLeader() {
				try {
					System.out.println(name + " is not Leader. id: " + latch.getId() + " leader: "
					      + latch.getLeader().getId());
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			@Override
			public void isLeader() {
				try {
					System.out.println(name + " is Leader. id: " + latch.getId() + " leader: " + latch.getLeader().getId());
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}, Executors.newSingleThreadExecutor(new ThreadFactory() {

			@Override
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r);
				t.setDaemon(true);
				return t;
			}
		}));

		latch.start();

		System.in.read();
		latch.close();
		client.close();
	}
}
