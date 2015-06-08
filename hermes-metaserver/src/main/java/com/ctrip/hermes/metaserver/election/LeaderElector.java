package com.ctrip.hermes.metaserver.election;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class LeaderElector extends LeaderSelectorListenerAdapter implements Closeable {
	private LeaderSelector m_leaderSelector;

	private String m_path;

	private String m_name;

	private AtomicBoolean m_started = new AtomicBoolean(false);

	public LeaderElector(CuratorFramework client, String name, String path) {
		m_name = name;
		m_path = path;

		m_leaderSelector = new LeaderSelector(client, m_path, this);
		m_leaderSelector.autoRequeue();
	}

	@Override
	public void takeLeadership(CuratorFramework client) throws Exception {
		// TODO
		System.out.println(m_name + " took leadership");
	}

	public void start() {
		if (m_started.compareAndSet(false, true)) {
			m_leaderSelector.start();
		}
	}

	@Override
	public void close() throws IOException {
		if (m_started.compareAndSet(true, false)) {
			m_leaderSelector.close();
		}

	}

}
