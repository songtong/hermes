package com.ctrip.hermes.metaserver;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;

import com.google.common.collect.Lists;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class LeaderTest {
	private static final int CLIENT_QTY = 2;

	private static final String PATH = "/examples/leader";

	public static void main(String[] args) throws Exception {
		// all of the useful sample code is in ExampleClient.java

		System.out
		      .println("Create "
		            + CLIENT_QTY
		            + " clients, have each negotiate for leadership and then wait a random number of seconds before letting another leader election occur.");
		System.out
		      .println("Notice that leader election is fair: all clients will become leader and will do so the same number of times.");

		List<CuratorFramework> clients = Lists.newArrayList();
		List<ExampleClient> examples = Lists.newArrayList();
		try {
			for (int i = 0; i < CLIENT_QTY; ++i) {
				CuratorFramework client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", 5000, 2000,
				      new BoundedExponentialBackoffRetry(1000, 5000, Integer.MAX_VALUE));
				clients.add(client);

				ExampleClient example = new ExampleClient(client, PATH, "Client #" + i);
				examples.add(example);

				client.start();
				example.start();
			}

			System.out.println("Press enter/return to quit\n");
			new BufferedReader(new InputStreamReader(System.in)).readLine();
		} finally {
			System.out.println("Shutting down...");

			for (ExampleClient exampleClient : examples) {
				CloseableUtils.closeQuietly(exampleClient);
			}
			for (CuratorFramework client : clients) {
				CloseableUtils.closeQuietly(client);
			}

		}
	}
}

/**
 * An example leader selector client. Note that {@link LeaderSelectorListenerAdapter} which has the recommended handling for
 * connection state issues
 */
class ExampleClient extends LeaderSelectorListenerAdapter implements Closeable {
	private final String name;

	private final LeaderSelector leaderSelector;

	private final AtomicInteger leaderCount = new AtomicInteger();

	public ExampleClient(CuratorFramework client, String path, String name) {
		this.name = name;

		// create a leader selector using the given path for management
		// all participants in a given leader selection must use the same path
		// ExampleClient here is also a LeaderSelectorListener but this isn't required
		leaderSelector = new LeaderSelector(client, path, this);

		leaderSelector.setId(name);

		// for most cases you will want your instance to requeue when it relinquishes leadership
		leaderSelector.autoRequeue();
	}

	public void start() throws IOException {
		// the selection for this instance doesn't start until the leader selector is started
		// leader selection is done in the background so this call to leaderSelector.start() returns immediately
		leaderSelector.start();
	}

	@Override
	public void close() throws IOException {
		leaderSelector.close();
	}

	@Override
	public void takeLeadership(CuratorFramework client) throws Exception {
		// we are now the leader. This method should not return until we want to relinquish leadership

		final int waitSeconds = (int) (5 * Math.random()) + 1;

		System.out.println(name + " is now the leader. Waiting " + waitSeconds + " seconds..." + " id: "
		      + leaderSelector.getId() + " leader:" + " " + leaderSelector.getLeader().getId());
		System.out.println(name + " has been leader " + leaderCount.getAndIncrement() + " time(s) before.");
		try {
			Thread.sleep(TimeUnit.SECONDS.toMillis(waitSeconds));
		} catch (InterruptedException e) {
			System.err.println(name + " was interrupted.");
			Thread.currentThread().interrupt();
		} finally {
			System.out.println(name + " relinquishing leadership.\n");
		}
	}
}
