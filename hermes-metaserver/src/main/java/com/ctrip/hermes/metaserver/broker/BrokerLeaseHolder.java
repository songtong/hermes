package com.ctrip.hermes.metaserver.broker;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.metaserver.broker.watcher.BrokerLeaseAddedWatcher;
import com.ctrip.hermes.metaserver.broker.watcher.BrokerLeaseChangedWatcher;
import com.ctrip.hermes.metaserver.commons.BaseLeaseHolder;
import com.ctrip.hermes.metaserver.commons.ClientLeaseInfo;
import com.ctrip.hermes.metaserver.commons.CompositeException;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = BrokerLeaseHolder.class)
public class BrokerLeaseHolder extends BaseLeaseHolder<Pair<String, Integer>> {

	private ExecutorService m_watcherExecutorService;

	private Set<String> m_watchedTopics = new HashSet<>();

	private BrokerLeaseChangedWatcher m_brokerLeaseChangedWatcher;

	private BrokerLeaseAddedWatcher m_brokerLeaseAddedWatcher;

	@Override
	protected String convertKeyToZkPath(Pair<String, Integer> topicPartition) {
		return ZKPathUtils.getBrokerLeaseZkPath(topicPartition.getKey(), topicPartition.getValue());
	}

	@Override
	protected String[] getZkPersistTouchPaths(Pair<String, Integer> topicPartition) {
		return new String[] { ZKPathUtils.getBrokerLeaseTopicParentZkPath(topicPartition.getKey()) };
	}

	@Override
	protected Pair<String, Integer> convertZkPathToKey(String path) {
		return ZKPathUtils.parseBrokerLeaseZkPath(path);
	}

	@Override
	protected Map<String, Map<String, ClientLeaseInfo>> loadExistingLeases() throws Exception {
		CuratorFramework client = m_zkClient.get();

		Map<String, Map<String, ClientLeaseInfo>> existingLeases = new ConcurrentHashMap<>();

		List<String> topics = client.getChildren()//
		      .usingWatcher(m_brokerLeaseAddedWatcher)//
		      .forPath(ZKPathUtils.getBrokerLeaseRootZkPath());
		m_brokerLeaseAddedWatcher.addWatchedPath(ZKPathUtils.getBrokerLeaseRootZkPath());

		loadLeasesConcurrently(existingLeases, topics);

		return existingLeases;
	}

	private void loadLeasesConcurrently(final Map<String, Map<String, ClientLeaseInfo>> existingLeases,
	      List<String> topics) throws InterruptedException {
		if (topics != null && !topics.isEmpty()) {
			ListeningExecutorService leaseLoadingThreadPool = MoreExecutors.listeningDecorator(Executors
			      .newFixedThreadPool(5, HermesThreadFactory.create("ConsumerLeaseHolder-Init-Temp", true)));

			final List<Throwable> innerThrowables = new LinkedList<>();
			final Object syncObj = new Object();
			final CountDownLatch latch = new CountDownLatch(topics.size());

			for (final String topic : topics) {
				ListenableFuture<Map<String, Map<String, ClientLeaseInfo>>> future = leaseLoadingThreadPool
				      .submit(new Callable<Map<String, Map<String, ClientLeaseInfo>>>() {

					      @Override
					      public Map<String, Map<String, ClientLeaseInfo>> call() throws Exception {
						      return loadAndWatchTopicExistingLeases(topic);
					      }
				      });

				Futures.addCallback(future, new FutureCallback<Map<String, Map<String, ClientLeaseInfo>>>() {

					@Override
					public void onSuccess(Map<String, Map<String, ClientLeaseInfo>> result) {
						existingLeases.putAll(result);
						latch.countDown();
					}

					@Override
					public void onFailure(Throwable t) {
						synchronized (syncObj) {
							innerThrowables.add(t);
						}
						latch.countDown();
					}
				}, leaseLoadingThreadPool);
			}

			latch.await();

			leaseLoadingThreadPool.shutdown();

			if (!innerThrowables.isEmpty()) {
				throw new CompositeException(innerThrowables);
			}
		}
	}

	public Map<String, Map<String, ClientLeaseInfo>> loadAndWatchTopicExistingLeases(String topic) throws Exception {
		Map<String, Map<String, ClientLeaseInfo>> topicExistingLeases = new HashMap<>();

		CuratorFramework client = m_zkClient.get();
		String topicPath = ZKPaths.makePath(ZKPathUtils.getBrokerLeaseRootZkPath(), topic);

		client.getData().usingWatcher(m_brokerLeaseChangedWatcher).forPath(topicPath);
		m_brokerLeaseChangedWatcher.addWatchedPath(topicPath);
		addWatchedTopic(topic);

		List<String> partitions = client.getChildren().forPath(topicPath);

		if (partitions != null && !partitions.isEmpty()) {
			for (String partition : partitions) {
				String partitionPath = ZKPaths.makePath(topicPath, partition);
				byte[] data = client.getData().forPath(partitionPath);
				Map<String, ClientLeaseInfo> existingLeases = deserializeExistingLeases(data);
				if (existingLeases != null) {
					topicExistingLeases.put(partitionPath, existingLeases);
				}
			}
		}

		return topicExistingLeases;
	}

	@Override
	protected void doInitialize() {
		m_watcherExecutorService = Executors.newSingleThreadExecutor(HermesThreadFactory.create("BrokerLeaseWatcher",
		      true));
		m_brokerLeaseChangedWatcher = new BrokerLeaseChangedWatcher(m_watcherExecutorService, this);
		m_brokerLeaseAddedWatcher = new BrokerLeaseAddedWatcher(m_watcherExecutorService, this);
	}

	public synchronized boolean topicWatched(String topic) {
		return m_watchedTopics.contains(topic);
	}

	public synchronized void addWatchedTopic(String topic) {
		m_watchedTopics.add(topic);
	}

	public synchronized void removeWatchedTopic(String topic) {
		m_watchedTopics.remove(topic);
	}

}
