package com.ctrip.hermes.portal.service.dashboard;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.metaservice.queue.MessagePriority;
import com.ctrip.hermes.metaservice.queue.MessageQueueDao;
import com.ctrip.hermes.metaservice.queue.OffsetMessage;
import com.ctrip.hermes.portal.config.PortalConstants;
import com.ctrip.hermes.portal.resource.view.TopicDelayDetailView.DelayDetail;

public class ConsumerBacklogCalculateTask implements Runnable {
	private static final Logger log = LoggerFactory.getLogger(DefaultDashboardService.class);
	private String topicName;
	private ConsumerGroup consumer;
	private int partitionId;
	private CountDownLatch latch;
	private List<DelayDetail> consumerDelay;
	private MessageQueueDao dao;

	public ConsumerBacklogCalculateTask(String topicName, ConsumerGroup consumer, int partitionId, CountDownLatch latch,
			MessageQueueDao dao, List<DelayDetail> consumerDelay) {
		this.topicName = topicName;
		this.consumer = consumer;
		this.partitionId = partitionId;
		this.latch = latch;
		this.consumerDelay = consumerDelay;
		this.dao = dao;
	}

	@Override
	public void run() {
		try {
			MessagePriority msgPriority = dao.getLatestProduced(topicName, partitionId, PortalConstants.PRIORITY_TRUE);
			MessagePriority msgNonPriority = dao.getLatestProduced(topicName, partitionId,
					PortalConstants.PRIORITY_FALSE);
			Long priorityMsgId = msgPriority == null ? 0 : msgPriority.getId();
			Long nonPriorityMsgId = msgNonPriority == null ? 0 : msgNonPriority.getId();
			Map<Integer, Pair<OffsetMessage, OffsetMessage>> offsetMsgMap = dao.getLatestConsumed(topicName,
					partitionId);
			Long priorityDelay = null;
			Long nonPriorityDelay = null;
			Long priorityMsgOffset = null;
			Long nonPriorityMsgOffset = null;
			Pair<OffsetMessage, OffsetMessage> offsets = offsetMsgMap.get(consumer.getId());
			if (offsets != null) {
				priorityMsgOffset = offsets.getKey() == null ? 0 : offsets.getKey().getOffset();
				nonPriorityMsgOffset = offsets.getValue() == null ? 0 : offsets.getValue().getOffset();
				priorityDelay = priorityMsgId - priorityMsgOffset;
				nonPriorityDelay = nonPriorityMsgId - nonPriorityMsgOffset;
			}
			DelayDetail delayDetail = new DelayDetail(consumer.getName(), partitionId);
			delayDetail.setPriorityDelay(priorityDelay);
			delayDetail.setNonPriorityDelay(nonPriorityDelay);
			delayDetail.setPriorityMsgId(priorityMsgId);
			delayDetail.setNonPriorityMsgId(nonPriorityMsgId);
			delayDetail.setPriorityMsgOffset(priorityMsgOffset);
			delayDetail.setNonPriorityMsgOffset(nonPriorityMsgOffset);
			consumerDelay.add(delayDetail);
		} catch (Exception e) {
			log.warn("Get delay of topic: {}, partition: {}, consumer: {} failed.", topicName, partitionId,
					consumer.getName(), e);
		} finally {
			latch.countDown();
		}
	}
}
