package com.ctrip.hermes.monitor.job.partition;

import io.netty.util.internal.ConcurrentSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
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
			List<TableContext> tableContexts = createTableContexts(meta, table2PartitionInfos, limits);
			partitionCheckerResult.setPartitionChangeListResult(doExecuteManagementJob(tableContexts));
			partitionCheckerResult.setPartitionInfo(generatePartitionInfoResult(tableContexts));
		} catch (Exception e) {
			log.error("Check partition status failed.", e);
		}
		return partitionCheckerResult;
	}

	private CheckerResult generatePartitionInfoResult(List<TableContext> tableContexts) {
		CheckerResult partitionInfos = new CheckerResult();
		PartitionInformationEvent event = new PartitionInformationEvent(tableContexts);
		partitionInfos.addMonitorEvent(event);
		partitionInfos.setRunSuccess(true);
		return partitionInfos;
	}

	private CheckerResult doExecuteManagementJob(List<TableContext> tableContexts) {
		CheckerResult changeResult = new CheckerResult();
		ExecutorService es = Executors.newFixedThreadPool(PARTITION_TASK_SIZE);
		try {
			CountDownLatch latch = new CountDownLatch(tableContexts.size());
			ConcurrentSet<Exception> exceptions = new ConcurrentSet<Exception>();
			for (TableContext ctx : tableContexts) {
				es.execute(new PartitionManagementTask(ctx, changeResult, latch, exceptions, m_partitionService));
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

		Map<String, Integer> includes = JSON.parseObject(m_config.getPartitionRetainInDay(),
		      new TypeReference<Map<String, Integer>>() {
		      });
		Set<String> excludes = JSON.parseObject(m_config.getPartitionCheckerExcludeTopics(),
		      new TypeReference<Set<String>>() {
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

	private List<TableContext> createTableContexts(//
	      Meta meta, Map<String, Pair<Datasource, List<PartitionInfo>>> partitions, Map<String, Integer> topicRetainDays) {
		int cordon = m_config.getPartitionWatermarkInDay();
		int increment = m_config.getPartitionIncrementInDay();

		List<TableContext> ctxes = new ArrayList<TableContext>();
		for (Topic topic : meta.getTopics().values()) {
			if (topicRetainDays.containsKey(topic.getName())) {
				int retain = topicRetainDays.get(topic.getName());
				for (Partition partition : topic.getPartitions()) {
					MessageTableContext pMsgCtx = new MessageTableContext(topic, partition, 0, retain, cordon, increment);
					if (partitions.containsKey(pMsgCtx.getTableName())) {
						ctxes.add(pMsgCtx.setPartitionInfos(partitions.get(pMsgCtx.getTableName()).getValue()));

						MessageTableContext npMsgCtx = new MessageTableContext(topic, partition, 1, retain, cordon, increment);
						ctxes.add(npMsgCtx.setPartitionInfos(partitions.get(npMsgCtx.getTableName()).getValue()));

						Datasource ds = partitions.get(pMsgCtx.getTableName()).getKey();
						pMsgCtx.setDatasource(ds);
						npMsgCtx.setDatasource(ds);

						for (ConsumerGroup consumer : topic.getConsumerGroups()) {
							ResendTableContext cCtx = new ResendTableContext(topic, partition, consumer, retain, cordon,
							      increment);
							Pair<Datasource, List<PartitionInfo>> pair = partitions.get(cCtx.getTableName());
							if (pair != null) {
								ctxes.add(cCtx.setPartitionInfos(pair.getValue()).setDatasource(ds));
							}
						}
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
