package com.ctrip.hermes.monitor.checker.mysql.task.partition;

import io.netty.util.internal.ConcurrentSet;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.metaservice.monitor.event.PartitionModificationEvent;
import com.ctrip.hermes.metaservice.monitor.event.PartitionModificationEvent.PartitionOperation;
import com.ctrip.hermes.monitor.checker.CheckerResult;
import com.ctrip.hermes.monitor.checker.mysql.dal.entity.PartitionInfo;
import com.ctrip.hermes.monitor.checker.mysql.task.partition.context.TableContext;
import com.ctrip.hermes.monitor.checker.mysql.task.partition.strategy.MessagePartitionCheckerStrategy;
import com.ctrip.hermes.monitor.checker.mysql.task.partition.strategy.PartitionCheckerStrategy;
import com.ctrip.hermes.monitor.checker.mysql.task.partition.strategy.PartitionCheckerStrategy.AnalysisResult;
import com.ctrip.hermes.monitor.checker.mysql.task.partition.strategy.ResendPartitionCheckerStrategy;
import com.ctrip.hermes.monitor.service.PartitionService;
import com.ctrip.hermes.monitor.utils.ApplicationContextUtil;

public class PartitionCheckerTask implements Runnable {
	private static final Logger log = LoggerFactory.getLogger(PartitionCheckerTask.class);

	private TableContext m_task;

	private CheckerResult m_result;

	private CountDownLatch m_latch;

	private ConcurrentSet<Exception> m_exceptions;

	private PartitionService m_service;

	public PartitionCheckerTask(TableContext task, CheckerResult result, CountDownLatch latch,
	      ConcurrentSet<Exception> exceptions, PartitionService service) {
		m_task = task;
		m_result = result;
		m_latch = latch;
		m_exceptions = exceptions;
		m_service = service;
	}

	@Override
	public void run() {
		try {
			AnalysisResult analysisResult = null;
			try {
				analysisResult = findStrategy(m_task).analysisTable(m_task);
			} catch (Exception e) {
				log.debug("Analysis table failed: {}", m_task, e);
			}
			if (analysisResult != null) {
				try {
					List<PartitionInfo> dropList = analysisResult.getDropList();
					if (dropList.size() > 0) {
						String sql = m_service.dropPartitions(m_task, dropList);
						m_result.addMonitorEvent(generateEvent(m_task, PartitionOperation.DROP, sql));
					}
				} catch (Exception e) {
					m_exceptions.add(e);
				}
				try {
					List<PartitionInfo> addList = analysisResult.getAddList();
					if (addList.size() > 0) {
						String sql = m_service.addPartitions(m_task, addList);
						m_result.addMonitorEvent(generateEvent(m_task, PartitionOperation.ADD, sql));
					}
				} catch (Exception e) {
					m_exceptions.add(e);
				}
			}
		} finally {
			m_latch.countDown();
		}
	}

	private PartitionModificationEvent generateEvent(TableContext ctx, PartitionOperation op, String sql) {
		PartitionModificationEvent e = new PartitionModificationEvent();
		e.setTopic(ctx.getTopic().getName());
		e.setPartition(ctx.getPartition().getId());
		e.setOp(op);
		e.setSql(sql);
		e.setTableName(ctx.getTableName());
		return e;
	}

	private PartitionCheckerStrategy findStrategy(TableContext ctx) {
		switch (ctx.getType()) {
		case MESSAGE:
			return ApplicationContextUtil.getBean(MessagePartitionCheckerStrategy.class);
		case RESEND:
			return ApplicationContextUtil.getBean(ResendPartitionCheckerStrategy.class);
		default:
			throw new RuntimeException("No such table type!");
		}
	}
}
