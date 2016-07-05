package com.ctrip.hermes.metaserver.consumer;

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

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.metaserver.commons.BaseLeaseHolder;
import com.ctrip.hermes.metaserver.commons.ClientLeaseInfo;
import com.ctrip.hermes.metaserver.commons.CompositeException;
import com.ctrip.hermes.metaserver.consumer.watcher.ConsumerLeaseAddedWatcher;
import com.ctrip.hermes.metaserver.consumer.watcher.ConsumerLeaseChangedWatcher;
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
@Named(type = ConsumerLeaseHolder.class)
public class ConsumerLeaseHolder extends BaseLeaseHolder<Tpg> {

	private ExecutorService m_watcherExecutor;

	private Set<String> m_watchedTopics = new HashSet<>();

	private ConsumerLeaseChangedWatcher m_consumerLeaseChangedWatcher;

	private ConsumerLeaseAddedWatcher m_consumerLeaseAddedWatcher;

	@Override
	protected String convertKeyToZkPath(Tpg tpg) {
		return ZKPathUtils.getConsumerLeaseZkPath(tpg.getTopic(), tpg.getPartition(), tpg.getGroupId());
	}

	@Override
	protected String[] getZkPersistTouchPaths(Tpg tpg) {
		return new String[] { ZKPathUtils.getConsumerLeaseTopicParentZkPath(tpg.getTopic()) };
	}

	@Override
	protected Tpg convertZkPathToKey(String path) {
		return ZKPathUtils.parseConsumerLeaseZkPath(path);
	}

	@Override
	protected Map<String, Map<String, ClientLeaseInfo>> loadExistingLeases() throws Exception {
		CuratorFramework client = m_zkClient.get();

		final Map<String, Map<String, ClientLeaseInfo>> existingLeases = new ConcurrentHashMap<>();

		List<String> topics = client.getChildren()//
		      .usingWatcher(m_consumerLeaseAddedWatcher)//
		      .forPath(ZKPathUtils.getConsumerLeaseRootZkPath());
		m_consumerLeaseAddedWatcher.addWatchedPath(ZKPathUtils.getConsumerLeaseRootZkPath());

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
		String topicPath = ZKPaths.makePath(ZKPathUtils.getConsumerLeaseRootZkPath(), topic);

		client.getData().usingWatcher(m_consumerLeaseChangedWatcher).forPath(topicPath);
		m_consumerLeaseChangedWatcher.addWatchedPath(topicPath);
		addWatchedTopic(topic);

		List<String> partitions = client.getChildren().forPath(topicPath);

		if (partitions != null && !partitions.isEmpty()) {
			for (String partition : partitions) {

				String partitionPath = ZKPaths.makePath(topicPath, partition);
				List<String> groups = client.getChildren().forPath(partitionPath);

				if (groups != null && !groups.isEmpty()) {
					for (String group : groups) {
						String groupPath = ZKPaths.makePath(partitionPath, group);
						byte[] data = client.getData().forPath(groupPath);
						Map<String, ClientLeaseInfo> existingLeases = deserializeExistingLeases(data);
						if (existingLeases != null) {
							topicExistingLeases.put(groupPath, existingLeases);
						}
					}
				}
			}
		}

		return topicExistingLeases;
	}

	@Override
	protected void doInitialize() {
		m_watcherExecutor = Executors.newSingleThreadExecutor(HermesThreadFactory.create("ConsumerLeaseWatcher", true));
		m_consumerLeaseChangedWatcher = new ConsumerLeaseChangedWatcher(m_watcherExecutor, this);
		m_consumerLeaseAddedWatcher = new ConsumerLeaseAddedWatcher(m_watcherExecutor, this);
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

	@Override
	protected String getName() {
		return "ConsumerLeaseHolder";
	}
}
