package com.ctrip.hermes.monitor.job.partition;

import io.netty.util.internal.ConcurrentSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.unidal.tuple.Pair;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.monitor.event.PartitionInformationEvent;
import com.ctrip.hermes.metaservice.queue.PartitionInfo;
import com.ctrip.hermes.metaservice.queue.TableContext;
import com.ctrip.hermes.monitor.checker.CheckerResult;
import com.ctrip.hermes.monitor.checker.exception.CompositeException;
import com.ctrip.hermes.monitor.config.MonitorConfig;
import com.ctrip.hermes.monitor.job.partition.context.DeadLetterTableContext;
import com.ctrip.hermes.monitor.job.partition.context.MessageTableContext;
import com.ctrip.hermes.monitor.job.partition.context.ResendTableContext;
import com.ctrip.hermes.monitor.service.PartitionService;
import com.ctrip.hermes.monitor.utils.MonitorUtils;
import com.ctrip.hermes.monitor.utils.MonitorUtils.Matcher;

@Component(value = PartitionManagementJob.ID)
public class PartitionManagementJob {

	private static final Logger log = LoggerFactory.getLogger(PartitionManagementJob.class);

	public static final String ID = "PartitionChecker";

	private static final int PARTITION_TASK_SIZE = 1;

	private static final int PARTITION_CHECKER_TIMEOUT_MINUTE = 720;

	@Autowired
	private PartitionService m_partitionService;

	@Autowired
	private MonitorConfig m_config;

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

	public PartitionCheckerResult check() {
		PartitionCheckerResult partitionCheckerResult = new PartitionCheckerResult();
		try {
			Meta meta = fetchMeta();
			Map<String, Integer> limits = parseLimits(meta);
			Map<String, Pair<Datasource, List<PartitionInfo>>> table2PartitionInfos = getPartitionInfosFromMeta(meta);
			sortPartitionsInOrdinal(table2PartitionInfos);
			List<TableContext> tableContexts = createTableContexts(meta, table2PartitionInfos, limits);
			ConcurrentHashMap<String, List<PartitionInfo>> wastes = new ConcurrentHashMap<String, List<PartitionInfo>>();
			partitionCheckerResult.setPartitionChangeListResult(doExecuteManagementJob(tableContexts, wastes));
			partitionCheckerResult.setPartitionInfo(generatePartitionInfoResult(tableContexts, wastes));
		} catch (Exception e) {
			log.error("Check partition status failed.", e);
		}
		return partitionCheckerResult;
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

	private Map<String, Pair<Datasource, List<PartitionInfo>>> getPartitionInfosFromMeta(Meta meta) throws Exception {
		Map<String, Pair<Datasource, List<PartitionInfo>>> table2PartitionInfos = new HashMap<>();
		Set<String> checkedDatasource = new HashSet<String>();
		for (Datasource ds : meta.getStorages().get(Storage.MYSQL).getDatasources()) {
			if (!checkedDatasource.contains(ds.getProperties().get("url").getValue())) {
				checkedDatasource.add(ds.getProperties().get("url").getValue());
				table2PartitionInfos.putAll(m_partitionService.queryDatasourcePartitions(ds));
			} else {
				log.info("Already checked datasource:{}", ds.getProperties());
			}
		}
		return table2PartitionInfos;
	}

	private Map<String, Integer> parseLimits(Meta meta) {
		Map<String, Integer> limits = new HashMap<String, Integer>();

		Map<String, Integer> includes = JSON.parseObject(m_config.getPartitionRetainInHour(),
		      new TypeReference<Map<String, Integer>>() {
		      });
		Set<String> excludes = JSON.parseObject(m_config.getPartitionCheckerExcludeTopics(), new TypeReference<Set<String>>() {
		});
		log.info("***** Partition manager config, include: {}", includes);
		log.info("***** Partition manager config, exclude: {}", excludes);
		if (includes.containsKey(".*")) {
			for (Entry<String, Topic> entry : findTopics(".*", meta)) {
				if (!excludes.contains(entry.getValue().getName())) {
					limits.put(entry.getValue().getName(), includes.get(".*"));
				}
			}
		}
		for (Entry<String, Integer> item : includes.entrySet()) {
			if (!item.getKey().equals(".*") && item.getValue() > 0) {
				for (Entry<String, Topic> entry : findTopics(item.getKey(), meta)) {
					limits.put(entry.getValue().getName(), item.getValue());
				}
			}
		}
		log.info("**** Partition manager config, effective: {}", limits.keySet());
		return limits;
	}

	private List<Entry<String, Topic>> findTopics(final String pattern, Meta meta) {
		return MonitorUtils.findMatched(meta.getTopics().entrySet(), new Matcher<Entry<String, Topic>>() {
			@Override
			public boolean match(Entry<String, Topic> obj) {
				return Pattern.matches(pattern, obj.getKey());
			}
		});
	}

	private void addMessageTableContext(Map<String, Pair<Datasource, List<PartitionInfo>>> partitions, //
	      Topic topic, Partition partition, int priority, int retain, List<TableContext> contexts) {
		MessageTableContext ctx = new MessageTableContext(topic, partition, priority, //
		      retain, m_config.getPartitionWatermarkInDay(), m_config.getPartitionIncrementInDay());
		Pair<Datasource, List<PartitionInfo>> pair = partitions.get(ctx.getTableName());
		if (pair != null) {
			contexts.add(ctx.setPartitionInfos(pair.getValue()).setDatasource(pair.getKey()));
		}
	}

	private void addResendTableContext(Map<String, Pair<Datasource, List<PartitionInfo>>> partitions, //
	      Topic topic, Partition partition, ConsumerGroup consumer, int retain, List<TableContext> contexts) {
		ResendTableContext ctx = new ResendTableContext(topic, partition, consumer, //
		      retain, m_config.getPartitionWatermarkInDay(), m_config.getPartitionIncrementInDay());
		Pair<Datasource, List<PartitionInfo>> pair = partitions.get(ctx.getTableName());
		if (pair != null) {
			contexts.add(ctx.setPartitionInfos(pair.getValue()).setDatasource(pair.getKey()));
		}
	}

	private void addDeadLetterTableContext(Map<String, Pair<Datasource, List<PartitionInfo>>> partitions, //
	      Topic topic, Partition partition, int retain, List<TableContext> contexts) {
		DeadLetterTableContext ctx = new DeadLetterTableContext(topic, partition, //
		      retain, m_config.getPartitionWatermarkInDay(), m_config.getPartitionIncrementInDay());
		Pair<Datasource, List<PartitionInfo>> pair = partitions.get(ctx.getTableName());
		if (pair != null) {
			contexts.add(ctx.setPartitionInfos(pair.getValue()).setDatasource(pair.getKey()));
		}
	}

	private List<TableContext> createTableContexts(//
	      Meta meta, Map<String, Pair<Datasource, List<PartitionInfo>>> partitions, Map<String, Integer> topicRetainHours) {
		List<TableContext> ctxes = new ArrayList<TableContext>();
		for (Topic topic : meta.getTopics().values()) {
			if (topicRetainHours.containsKey(topic.getName())) {
				int retain = topicRetainHours.get(topic.getName());
				for (Partition partition : topic.getPartitions()) {
					addMessageTableContext(partitions, topic, partition, 0, retain, ctxes);
					addMessageTableContext(partitions, topic, partition, 1, retain, ctxes);
					addDeadLetterTableContext(partitions, topic, partition, retain * 5, ctxes);
					for (ConsumerGroup consumer : topic.getConsumerGroups()) {
						addResendTableContext(partitions, topic, partition, consumer, retain, ctxes);
					}
				}
			}
		}
		return ctxes;
	}

	protected Meta fetchMeta() {
		try {
			return JSON.parseObject(MonitorUtils.curl(m_config.getMetaRestUrl(), 3000, 1000), Meta.class);
		} catch (Exception e) {
			throw new RuntimeException("Fetch meta failed.", e);
		}
	}
}
