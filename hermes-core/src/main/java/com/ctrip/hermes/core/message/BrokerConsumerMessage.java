package com.ctrip.hermes.core.message;

import io.netty.channel.Channel;

import java.util.Iterator;

import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.transport.command.v2.AckMessageCommandV2;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class BrokerConsumerMessage<T> implements ConsumerMessage<T>, PropertiesHolderAware, BaseConsumerMessageAware<T> {

	private BaseConsumerMessage<T> m_baseMsg;

	private long m_msgSeq;

	private int m_partition;

	private boolean m_priority;

	private boolean m_resend = false;

	private String m_groupId;

	private long m_correlationId;

	private Channel m_channel;

	private int m_retryTimesOfRetryPolicy;

	private boolean m_ackWithForwardOnly = false;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public BrokerConsumerMessage(BaseConsumerMessage baseMsg) {
		m_baseMsg = baseMsg;
	}

	public boolean isAckWithForwardOnly() {
		return m_ackWithForwardOnly;
	}

	public void setAckWithForwardOnly(boolean ackWithForwardOnly) {
		m_ackWithForwardOnly = ackWithForwardOnly;
	}

	public String getGroupId() {
		return m_groupId;
	}

	public void setGroupId(String groupId) {
		m_groupId = groupId;
	}

	public long getCorrelationId() {
		return m_correlationId;
	}

	public void setCorrelationId(long correlationId) {
		m_correlationId = correlationId;
	}

	public void setChannel(Channel channel) {
		m_channel = channel;
	}

	public boolean isPriority() {
		return m_priority;
	}

	public void setPriority(boolean priority) {
		m_priority = priority;
	}

	public int getPartition() {
		return m_partition;
	}

	public void setPartition(int partition) {
		m_partition = partition;
	}

	public long getMsgSeq() {
		return m_msgSeq;
	}

	public void setMsgSeq(long msgSeq) {
		this.m_msgSeq = msgSeq;
	}

	@Override
	public void nack() {
		if (m_baseMsg.nack()) {
			AckMessageCommandV2 cmd = createAckCommand();
			cmd.getHeader().setCorrelationId(m_correlationId);
			Tpp tpp = new Tpp(getTopic(), getPartition(), m_priority);
			cmd.addNackMsg(tpp, m_groupId, m_resend, m_msgSeq, m_baseMsg.getRemainingRetries(),
			      m_baseMsg.getOnMessageStartTimeMills(), m_baseMsg.getOnMessageEndTimeMills());
			m_channel.writeAndFlush(cmd);
		}
	}

	private AckMessageCommandV2 createAckCommand() {
		return m_ackWithForwardOnly ? new AckMessageCommandV2(AckMessageCommandV2.FORWARD_ONLY)
		      : new AckMessageCommandV2(AckMessageCommandV2.NORMAL);
	}

	@Override
	public String getProperty(String name) {
		return m_baseMsg.getDurableAppProperty(name);
	}

	@Override
	public Iterator<String> getPropertyNames() {
		return m_baseMsg.getRawDurableAppPropertyNames();
	}

	@Override
	public long getBornTime() {
		return m_baseMsg.getBornTime();
	}

	@Override
	public String getTopic() {
		return m_baseMsg.getTopic();
	}

	@Override
	public String getRefKey() {
		return m_baseMsg.getRefKey();
	}

	@Override
	public T getBody() {
		return m_baseMsg.getBody();
	}

	@Override
	public void ack() {
		if (m_baseMsg.ack()) {
			AckMessageCommandV2 cmd = createAckCommand();
			cmd.getHeader().setCorrelationId(m_correlationId);
			Tpp tpp = new Tpp(getTopic(), getPartition(), m_priority);
			cmd.addAckMsg(tpp, m_groupId, m_resend, m_msgSeq, m_baseMsg.getRemainingRetries(),
			      m_baseMsg.getOnMessageStartTimeMills(), m_baseMsg.getOnMessageEndTimeMills());
			m_channel.writeAndFlush(cmd);
		}
	}

	@Override
	public MessageStatus getStatus() {
		return m_baseMsg.getStatus();
	}

	public void setResend(boolean resend) {
		m_resend = resend;
	}

	public boolean isResend() {
		return m_resend;
	}

	public int getRemainingRetries() {
		return m_baseMsg.getRemainingRetries();
	}

	public PropertiesHolder getPropertiesHolder() {
		return m_baseMsg.getPropertiesHolder();
	}

	@Override
	public String toString() {
		return "BrokerConsumerMessage{" + "m_baseMsg=" + m_baseMsg + ", m_msgSeq=" + m_msgSeq + ", m_partition="
		      + m_partition + ", m_priority=" + m_priority + ", m_resend=" + m_resend + ", m_groupId='" + m_groupId
		      + '\'' + ", m_correlationId=" + m_correlationId + ", m_channel=" + m_channel + '}';
	}

	@Override
	public BaseConsumerMessage<T> getBaseConsumerMessage() {
		return m_baseMsg;
	}

	public long getOffset() {
		return this.getMsgSeq();
	}

	public int getRetryTimesOfRetryPolicy() {
		return m_retryTimesOfRetryPolicy;
	}

	public void setRetryTimesOfRetryPolicy(int retryTimesOfRetryPolicy) {
		m_retryTimesOfRetryPolicy = retryTimesOfRetryPolicy;
	}

	@Override
	public int getResendTimes() {
		if (isResend()) {
			return m_retryTimesOfRetryPolicy - m_baseMsg.getRemainingRetries() + 1;
		} else {
			return 0;
		}
	}
}
