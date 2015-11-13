package com.ctrip.hermes.monitor.checker.mysql;

import io.netty.util.internal.ConcurrentSet;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import com.ctrip.hermes.monitor.checker.CheckerResult;
import com.ctrip.hermes.monitor.checker.exception.CompositeException;
import com.ctrip.hermes.monitor.checker.mysql.dal.entity.PartitionInfo;
import com.ctrip.hermes.monitor.checker.mysql.task.partition.PartitionCheckerTask;
import com.ctrip.hermes.monitor.checker.mysql.task.partition.context.MessageTableContext;
import com.ctrip.hermes.monitor.checker.mysql.task.partition.context.ResendTableContext;
import com.ctrip.hermes.monitor.checker.mysql.task.partition.context.TableContext;
import com.ctrip.hermes.monitor.service.PartitionService;

@Component(value = PartitionChecker.ID)
public class PartitionChecker extends DBBasedChecker {

	private static final Logger log = LoggerFactory.getLogger(PartitionChecker.class);

	public static final String ID = "PartitionChecker";

	private static final int PARTITION_TASK_SIZE = 20;

	private static final int PARTITION_CHECKER_TIMEOUT_MINUTE = 720;

	@Autowired
	private PartitionService m_partitionService;

	@Override
	public String name() {
		return ID;
	}

	public static enum TableType {
		MESSAGE, RESEND;
	}

	@Override
	public CheckerResult check(Date toDate, int minutesBefore) {
		CheckerResult result = new CheckerResult();
		ExecutorService es = Executors.newFixedThreadPool(PARTITION_TASK_SIZE);
		try {
			Meta meta = fetchMeta();
			Map<String, Integer> limits = parseLimits(meta, m_config.getPartitionRetainInDay());
			Map<String, Pair<Datasource, List<PartitionInfo>>> ps = new HashMap<>();
			for (Datasource ds : meta.getStorages().get(Storage.MYSQL).getDatasources()) {
				ps.putAll(m_partitionService.getDatasourcePartitions(ds));
			}
			List<TableContext> tasks = getTableContexts(meta, ps, limits);
			CountDownLatch latch = new CountDownLatch(tasks.size());
			ConcurrentSet<Exception> exceptions = new ConcurrentSet<Exception>();
			for (TableContext task : tasks) {
				es.execute(new PartitionCheckerTask(task, result, latch, exceptions, m_partitionService));
			}
			if (latch.await(PARTITION_CHECKER_TIMEOUT_MINUTE, TimeUnit.MINUTES)) {
				result.setRunSuccess(true);
			} else {
				result.setRunSuccess(false);
				result.setErrorMessage("Check partition status timeout, check result is not completely.");
			}
			if (exceptions.size() > 0) {
				result.setRunSuccess(false);
				result.setErrorMessage("Check partition status task has exceptions!");
				result.setException(new CompositeException(exceptions));
			}
		} catch (Exception e) {
			log.error("Check partition status failed.", e);
			result.setException(e);
		} finally {
			es.shutdownNow();
		}
		return result;
	}

	private Map<String, Integer> parseLimits(Meta meta, String partitionRetainInDay) {
		Map<String, Integer> limits = new HashMap<String, Integer>();
		Map<String, Integer> includes = JSON.parseObject(partitionRetainInDay, new TypeReference<Map<String, Integer>>() {
		});
		if (includes.containsKey(".*")) {
			for (Entry<String, Topic> entry : findTopics(".*", meta)) {
				limits.put(entry.getValue().getName(), includes.get(".*"));
			}
		}
		for (Entry<String, Integer> item : includes.entrySet()) {
			if (!item.getKey().equals(".*") && item.getValue() > 0) {
				for (Entry<String, Topic> entry : findTopics(item.getKey(), meta)) {
					limits.put(entry.getValue().getName(), item.getValue());
				}
			}
		}
		return limits;
	}

	private List<Entry<String, Topic>> findTopics(final String pattern, Meta meta) {
		return findMatched(meta.getTopics().entrySet(), new Matcher<Entry<String, Topic>>() {
			@Override
			public boolean match(Entry<String, Topic> obj) {
				return Pattern.matches(pattern, obj.getKey());
			}
		});
	}

	private List<TableContext> getTableContexts(//
	      Meta meta, Map<String, Pair<Datasource, List<PartitionInfo>>> partitions, Map<String, Integer> topicRetainDays) {
		int cordon = m_config.getPartitionWatermarkInDay();
		int increment = m_config.getPartitionIncrementInDay();

		List<TableContext> ctxes = new ArrayList<TableContext>();
		for (Topic topic : meta.getTopics().values()) {
			int retain = topicRetainDays.get(topic.getName());
			for (Partition partition : topic.getPartitions()) {
				MessageTableContext pMsgCtx = new MessageTableContext(topic, partition, 0, retain, cordon, increment);
				ctxes.add(pMsgCtx.setPartitionInfos(partitions.get(pMsgCtx.getTableName()).getValue()));

				MessageTableContext npMsgCtx = new MessageTableContext(topic, partition, 1, retain, cordon, increment);
				ctxes.add(npMsgCtx.setPartitionInfos(partitions.get(npMsgCtx.getTableName()).getValue()));

				Datasource ds = partitions.get(pMsgCtx.getTableName()).getKey();
				pMsgCtx.setDatasource(ds);
				npMsgCtx.setDatasource(ds);

				for (ConsumerGroup consumer : topic.getConsumerGroups()) {
					ResendTableContext cCtx = new ResendTableContext(topic, partition, consumer, retain, cordon, increment);
					Pair<Datasource, List<PartitionInfo>> pair = partitions.get(cCtx.getTableName());
					if (pair != null) {
						ctxes.add(cCtx.setPartitionInfos(pair.getValue()).setDatasource(ds));
					}
				}
			}
		}
		return ctxes;
	}
}