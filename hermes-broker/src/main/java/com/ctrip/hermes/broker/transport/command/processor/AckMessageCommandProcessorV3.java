package com.ctrip.hermes.broker.transport.command.processor;

import java.util.Arrays;
import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.broker.queue.AckMessagesTask;
import com.ctrip.hermes.broker.queue.MessageQueueManager;
import com.ctrip.hermes.core.bo.AckContext;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.log.BizEvent;
import com.ctrip.hermes.core.log.FileBizLogger;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.core.transport.command.v3.AckMessageCommandV3;
import com.ctrip.hermes.core.transport.netty.NettyUtils;
import com.ctrip.hermes.core.utils.CollectionUtil;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class AckMessageCommandProcessorV3 implements CommandProcessor {

	@Inject
	private FileBizLogger m_bizLogger;

	@Inject
	private MessageQueueManager m_messageQueueManager;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.MESSAGE_ACK_V3);
	}

	@Override
	public void process(CommandProcessorContext ctx) {
		AckMessageCommandV3 reqCmd = (AckMessageCommandV3) ctx.getCommand();
		String consumerIp = NettyUtils.parseChannelRemoteAddr(ctx.getChannel(), false);

		String topic = reqCmd.getTopic();
		int partition = reqCmd.getPartition();
		String groupId = reqCmd.getGroup();

		List<AckContext> ackedPriorityContexts = reqCmd.getAckedMsgs().get(0);
		List<AckContext> ackedContexts = reqCmd.getAckedMsgs().get(1);
		List<AckContext> ackedResendContexts = reqCmd.getAckedResendMsgs().get(1);

		List<AckContext> nackedPriorityContexts = reqCmd.getNackedMsgs().get(0);
		List<AckContext> nackedContexts = reqCmd.getNackedMsgs().get(1);
		List<AckContext> nackedResendContexts = reqCmd.getNackedResendMsgs().get(1);

		logAcked(consumerIp, topic, partition, groupId, ackedPriorityContexts, ackedContexts, ackedResendContexts);
		logNacked(consumerIp, topic, partition, groupId, nackedPriorityContexts, nackedContexts, nackedResendContexts);

		AckMessagesTask task = new AckMessagesTask(topic, partition, groupId, reqCmd.getHeader().getCorrelationId(),
		      ctx.getChannel());
		task.setAckedContexts(ackedContexts);
		task.setAckedPriorityContexts(ackedPriorityContexts);
		task.setAckedResendContexts(ackedResendContexts);
		task.setNackedContexts(nackedContexts);
		task.setNackedPriorityContexts(nackedPriorityContexts);
		task.setNackedResendContexts(nackedResendContexts);
		m_messageQueueManager.submitAckMessagesTask(task);
	}

	private void logNacked(String consumerIp, String topic, int partition, String groupId,
	      List<AckContext> nackedPriorityContexts, List<AckContext> nackedContexts, List<AckContext> nackedResendContexts) {
		// priority
		bizLog(new Tpp(topic, partition, true), consumerIp, groupId, nackedPriorityContexts, false, false);
		// non-priority
		bizLog(new Tpp(topic, partition, false), consumerIp, groupId, nackedContexts, false, false);
		// resend
		bizLog(new Tpp(topic, partition, false), consumerIp, groupId, nackedResendContexts, true, false);
	}

	private void logAcked(String consumerIp, String topic, int partition, String groupId,
	      List<AckContext> ackedPriorityContexts, List<AckContext> ackedContexts, List<AckContext> ackedResendContexts) {
		// priority
		bizLog(new Tpp(topic, partition, true), consumerIp, groupId, ackedPriorityContexts, false, true);
		// non-priority
		bizLog(new Tpp(topic, partition, false), consumerIp, groupId, ackedContexts, false, true);
		// resend
		bizLog(new Tpp(topic, partition, false), consumerIp, groupId, ackedResendContexts, true, true);
	}

	private void bizLog(Tpp tpp, String consumerIp, String groupId, List<AckContext> ackContexts, boolean isResend,
	      boolean ack) {
		if (CollectionUtil.isNotEmpty(ackContexts)) {
			for (AckContext ctx : ackContexts) {
				BizEvent bizStartEvent = new BizEvent("Message.BizProcessStart", ctx.getOnMessageStartTimeMillis());
				addBizData(bizStartEvent, tpp, consumerIp, groupId, ctx, isResend, ack);
				m_bizLogger.log(bizStartEvent);

				BizEvent bizEndEvent = new BizEvent("Message.BizProcessEnd", ctx.getOnMessageEndTimeMillis());
				addBizData(bizEndEvent, tpp, consumerIp, groupId, ctx, isResend, ack);
				m_bizLogger.log(bizEndEvent);

				BizEvent ackEvent = new BizEvent("Message.Acked");
				addBizData(ackEvent, tpp, consumerIp, groupId, ctx, isResend, ack);
				addConsumerProcessTime(ackEvent, ctx.getOnMessageEndTimeMillis() - ctx.getOnMessageStartTimeMillis());
				m_bizLogger.log(ackEvent);
			}
		}
	}

	private void addConsumerProcessTime(BizEvent event, long processTime) {
		event.addData("processTime", processTime);
	}

	private void addBizData(BizEvent event, Tpp tpp, String consumerIp, String groupId, AckContext ctx,
	      boolean isResend, boolean ack) {
		event.addData("topic", tpp.getTopic());
		event.addData("partition", tpp.getPartition());
		event.addData("priority", tpp.getPriorityInt());
		event.addData("msgId", ctx.getMsgSeq());
		event.addData("consumerIp", consumerIp);
		event.addData("groupId", groupId);
		event.addData("isResend", isResend);
		event.addData("ack", ack);
		if (isResend) {
			event.addData("remainingRetries", ctx.getRemainingRetries());
		}
	}
}
