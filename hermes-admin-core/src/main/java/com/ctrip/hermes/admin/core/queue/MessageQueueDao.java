package com.ctrip.hermes.admin.core.queue;

import java.util.List;
import java.util.Map;

import org.unidal.dal.jdbc.DalException;
import org.unidal.tuple.Pair;

public interface MessageQueueDao {

	public MessagePriority getLatestProduced(String topic, int partition, int priority) throws DalException;

	/**
	 * 
	 * @param topic
	 * @param partition
	 * @return key=offsetMsg.GroupId, value.key=offsetMsg(priority),
	 *         value.value=offsetMsg(non-priority).
	 * @throws DalException
	 */
	public Map<Integer, Pair<OffsetMessage, OffsetMessage>> getLatestConsumed(String topic, int partition)
			throws DalException;

	public List<MessagePriority> getLatestMessages(String topic, int pratition, int count) throws DalException;

	public MessagePriority getMsgById(String topic, int partition, int priority, long id) throws DalException;

	public ResendGroupId getMaxResend(String topic, int partition, int groupId) throws DalException;

	public Map<Integer, OffsetResend> getLatestResend(String topic, int partition) throws DalException;
}
