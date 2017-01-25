package com.ctrip.hermes.monitor.job.partition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.admin.core.monitor.event.PartitionInformationEvent;
import com.ctrip.hermes.admin.core.queue.PartitionInfo;
import com.ctrip.hermes.admin.core.queue.TableContext;
import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.monitor.checker.CheckerResult;
import com.ctrip.hermes.monitor.checker.exception.CompositeException;
import com.ctrip.hermes.monitor.config.PartitionCheckerConfig;
import com.ctrip.hermes.monitor.job.partition.context.AbandonedTableContext;
import com.ctrip.hermes.monitor.job.partition.context.DeadLetterTableContext;
import com.ctrip.hermes.monitor.job.partition.context.MessageTableContext;
import com.ctrip.hermes.monitor.job.partition.context.ResendTableContext;
import com.ctrip.hermes.monitor.service.PartitionService;
import com.ctrip.hermes.monitor.utils.MonitorUtils;

import io.netty.util.internal.ConcurrentSet;

@Component(value = PartitionManagementJob.ID)
public class PartitionManagementJob {

	private static final Logger log = LoggerFactory.getLogger(PartitionManagementJob.class);

	public static final String ID = "PartitionChecker";

	private static final int PARTITION_TASK_SIZE = 1;

	private static final int PARTITION_CHECKER_TIMEOUT_MINUTE = 720;

	@Autowired
	private PartitionService m_partitionService;

	@Autowired
	private PartitionCheckerConfig m_config;

	public static class PartitionCheckerResult {
		private CheckerResult m_partitionInfo;

		private CheckerResult m_partitionChangeListResult;

		public CheckerResult getPartitionInfo() {
			return m_partitionInfo;
		}

		public void setPartitionInfo(CheckerResult partitionInfo) {
			m_partitionInfo = partitionInfo;
		}

		public CheckerResult getPartitionChangeListResult() {
			return m_partitionChangeListResult;
		}

		public void setPartitionChangeListResult(CheckerResult partitionChangeList) {
			m_partitionChangeListResult = partitionChangeList;
		}
	}

	public List<PartitionCheckerResult> check() {
		ConcurrentLinkedQueue<PartitionCheckerResult> partitionCheckerResults = new ConcurrentLinkedQueue<PartitionCheckerResult>();
		ExecutorService executor = null;
		try {
			Meta meta = fetchMeta();
			Map<Datasource, Map<String, Pair<Datasource, List<PartitionInfo>>>> pInfos = getPartitionInfosFromMeta(meta);
			executor = Executors.newFixedThreadPool(pInfos.size());
			CountDownLatch latch = new CountDownLatch(pInfos.size());
			try {
				for (Map.Entry<Datasource, Map<String, Pair<Datasource, List<PartitionInfo>>>> entry : pInfos.entrySet()) {
					sortPartitionsInOrdinal(entry.getValue());
					List<TableContext> tableContexts = createTableContexts(entry.getKey(), meta, entry.getValue());
					executor.execute(new DatasourcePartitionJob(latch, partitionCheckerResults, tableContexts));
				}
			} finally {
				latch.await();
			}
		} catch (Exception e) {
			log.error("Check partition status failed.", e);
		} finally {
			if (executor != null) {
				executor.shutdown();
			}
		}
		return Arrays.asList(partitionCheckerResults.toArray(new PartitionCheckerResult[partitionCheckerResults.size()]));
	}

	private class DatasourcePartitionJob implements Runnable {
		private CountDownLatch m_latch;

		private ConcurrentLinkedQueue<PartitionCheckerResult> m_results;

		List<TableContext> m_ctxes;

		public DatasourcePartitionJob( //
		      CountDownLatch latch, ConcurrentLinkedQueue<PartitionCheckerResult> results, List<TableContext> ctxes) {
			m_latch = latch;
			m_ctxes = ctxes;
			m_results = results;
		}

		@Override
		public void run() {
			try {
				ConcurrentHashMap<String, List<PartitionInfo>> wastes = new ConcurrentHashMap<String, List<PartitionInfo>>();
				PartitionCheckerResult partitionCheckerResult = new PartitionCheckerResult();
				partitionCheckerResult.setPartitionChangeListResult(doExecuteManagementJob(m_ctxes, wastes));
				partitionCheckerResult.setPartitionInfo(generatePartitionInfoResult(m_ctxes, wastes));
				m_results.add(partitionCheckerResult);
			} catch (Exception e) {
				log.error("Execute datasource partition management job failed.", e);
			} finally {
				m_latch.countDown();
			}
		}
	}

	private void sortPartitionsInOrdinal(Map<String, Pair<Datasource, List<PartitionInfo>>> table2PartitionInfos) {
		for (Entry<String, Pair<Datasource, List<PartitionInfo>>> entry : table2PartitionInfos.entrySet()) {
			Collections.sort(entry.getValue().getValue(), new Comparator<PartitionInfo>() {
				@Override
				public int compare(PartitionInfo o1, PartitionInfo o2) {
					return o1.getOrdinal() - o2.getOrdinal();
				}
			});
		}
	}

	private CheckerResult generatePartitionInfoResult( //
	      List<TableContext> tableContexts, ConcurrentHashMap<String, List<PartitionInfo>> wastes) {
		CheckerResult partitionInfos = new CheckerResult();
		PartitionInformationEvent event = new PartitionInformationEvent(tableContexts, wastes);
		partitionInfos.addMonitorEvent(event);
		partitionInfos.setRunSuccess(true);
		return partitionInfos;
	}

	private CheckerResult doExecuteManagementJob( //
	      List<TableContext> tableContexts, ConcurrentHashMap<String, List<PartitionInfo>> wastePartitionInfos) {
		CheckerResult changeResult = new CheckerResult();
		ExecutorService es = Executors.newFixedThreadPool(PARTITION_TASK_SIZE);
		try {
			CountDownLatch latch = new CountDownLatch(tableContexts.size());
			ConcurrentSet<Exception> exceptions = new ConcurrentSet<Exception>();
			for (TableContext ctx : tableContexts) {
				es.execute(//
				new PartitionManagementTask(ctx, changeResult, wastePartitionInfos, latch, exceptions, m_partitionService));
			}
			if (latch.await(PARTITION_CHECKER_TIMEOUT_MINUTE, TimeUnit.MINUTES)) {
				changeResult.setRunSuccess(true);
			} else {
				changeResult.setRunSuccess(false);
				changeResult.setErrorMessage("Check partition status timeout, check result is not completely.");
			}
			if (exceptions.size() > 0) {
				changeResult.setRunSuccess(false);
				changeResult.setErrorMessage("Check partition status task has exceptions!");
				changeResult.setException(new CompositeException(exceptions));
			}
		} catch (Exception e) {
			log.error("Check partition status failed.", e);
			changeResult.setException(e);
		} finally {
			es.shutdownNow();
		}
		return changeResult;
	}

	private Map<Datasource, Map<String, Pair<Datasource, List<PartitionInfo>>>> getPartitionInfosFromMeta(Meta meta)
	      throws Exception {
		Map<Datasource, Map<String, Pair<Datasource, List<PartitionInfo>>>> table2PartitionInfos = new HashMap<>();
		Set<String> checkedDatasource = new HashSet<String>();
		for (Datasource ds : meta.getStorages().get(Storage.MYSQL).getDatasources()) {
			if (!checkedDatasource.contains(ds.getProperties().get("url").getValue())) {
				checkedDatasource.add(ds.getProperties().get("url").getValue());
				Map<String, Pair<Datasource, List<PartitionInfo>>> tablePartitions = m_partitionService
				      .queryDatasourcePartitions(ds);
				table2PartitionInfos.put(ds, tablePartitions);
			}
		}
		return table2PartitionInfos;
	}

	private void addMessageTableContext(Map<String, Pair<Datasource, List<PartitionInfo>>> partitions, //
	      Topic topic, Partition partition, int priority, List<TableContext> contexts) {
		MessageTableContext ctx = new MessageTableContext(topic, partition, priority, //
		      m_config.getRetentionHour(topic.getName()), m_config.getPreAllocateLimitlineInDay(),
		      m_config.getPreAllocateSizeInDay());
		Pair<Datasource, List<PartitionInfo>> pair = partitions.get(ctx.getTableName());
		if (pair != null) {
			contexts.add(ctx.setPartitionInfos(pair.getValue()).setDatasource(pair.getKey()));
		}
	}

	private void addResendTableContext(Map<String, Pair<Datasource, List<PartitionInfo>>> partitions, //
	      Topic topic, Partition partition, ConsumerGroup consumer, List<TableContext> contexts) {
		ResendTableContext ctx = new ResendTableContext(topic, partition, consumer, //
		      m_config.getRetentionHour(topic.getName()), m_config.getPreAllocateLimitlineInDay(),
		      m_config.getPreAllocateSizeInDay());
		Pair<Datasource, List<PartitionInfo>> pair = partitions.get(ctx.getTableName());
		if (pair != null) {
			contexts.add(ctx.setPartitionInfos(pair.getValue()).setDatasource(pair.getKey()));
		}
	}

	private void addDeadLetterTableContext(Map<String, Pair<Datasource, List<PartitionInfo>>> partitions, //
	      Topic topic, Partition partition, List<TableContext> contexts) {
		DeadLetterTableContext ctx = new DeadLetterTableContext(topic, partition, //
		      m_config.getRetentionHour(topic.getName()) * 5, m_config.getPreAllocateLimitlineInDay(),
		      m_config.getPreAllocateSizeInDay());
		Pair<Datasource, List<PartitionInfo>> pair = partitions.get(ctx.getTableName());
		if (pair != null) {
			contexts.add(ctx.setPartitionInfos(pair.getValue()).setDatasource(pair.getKey()));
		}
	}

	private List<TableContext> createTableContexts(Datasource datasource, Meta meta,
	      Map<String, Pair<Datasource, List<PartitionInfo>>> partitions) {
		List<TableContext> ctxes = new ArrayList<TableContext>();
		Map<Long, Topic> id2topics = new HashMap<>();
		for (Topic topic : meta.getTopics().values()) {
			id2topics.put(topic.getId(), topic);
		}

		addAbandonedHermesTables(meta, datasource, id2topics, ctxes, partitions);

		for (Topic topic : meta.getTopics().values()) {
			if (!m_config.isTopicExcluded(topic.getName())) {
				for (Partition partition : topic.getPartitions()) {
					if (isTopicPartitionActive(meta, datasource, id2topics, topic.getId(), partition.getId())) {
						addMessageTableContext(partitions, topic, partition, 0, ctxes);
						addMessageTableContext(partitions, topic, partition, 1, ctxes);
						addDeadLetterTableContext(partitions, topic, partition, ctxes);
						for (ConsumerGroup consumer : topic.getConsumerGroups()) {
							addResendTableContext(partitions, topic, partition, consumer, ctxes);
						}
					}
				}
			}
		}
		return ctxes;
	}

	private Pair<Long, Integer> getTopicPartition(String tableName) {
		String[] parts = tableName.split("_");
		return new Pair<Long, Integer>(Long.valueOf(parts[0]), Integer.valueOf(parts[1]));
	}

	private String getPartitionReadDsUrl(Meta meta, Partition p) {
		if (p != null) {
			for (Datasource ds : meta.getStorages().get(Storage.MYSQL).getDatasources()) {
				if (ds.getId().equals(p.getReadDatasource())) {
					return ds.getProperties().get("url").getValue().toLowerCase();
				}
			}
		}
		return null;
	}

	private boolean isTopicPartitionActive(Meta meta, Datasource ds, Map<Long, Topic> id2topics, long topicId,
	      int partitionId) {
		Topic topic = id2topics.get(topicId);
		if (topic != null) {
			Partition p = topic.findPartition(partitionId);
			if (ds.getProperties().get("url").getValue().toLowerCase().equals(getPartitionReadDsUrl(meta, p))) {
				return true;
			}
		}
		return false;
	}

	private void addAbandonedHermesTables(Meta meta, Datasource datasource, Map<Long, Topic> id2topics,
	      List<TableContext> ctxes, Map<String, Pair<Datasource, List<PartitionInfo>>> partitions) {
		for (Entry<String, Pair<Datasource, List<PartitionInfo>>> entry : partitions.entrySet()) {
			String tableName = entry.getKey();
			if (MessageTableContext.isMessageTable(tableName) //
			      || ResendTableContext.isResendTable(tableName) //
			      || DeadLetterTableContext.isDeadTable(tableName)) {
				Pair<Long, Integer> tp = getTopicPartition(tableName);
				if (!isTopicPartitionActive(meta, datasource, id2topics, tp.getKey(), tp.getValue())) {
					ctxes.add(new AbandonedTableContext(tableName).setDatasource(datasource));
				}
			}
		}
	}

	protected Meta fetchMeta() {
		Meta meta = null;
		try {
			meta = MonitorUtils.fetchMeta();
			// return JSON.parseObject(MonitorUtils.curl(m_config.getMetaRestUrl(), 3000, 1000), Meta.class);
		} catch (Exception e) {
			throw new RuntimeException("Fetch meta failed.", e);
		}
		if (meta == null || meta.getTopics().size() <= 0) {
			throw new RuntimeException("Meta is null or topics is empty.");
		}
		return meta;
	}
}
