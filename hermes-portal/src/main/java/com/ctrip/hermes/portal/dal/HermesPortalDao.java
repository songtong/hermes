package com.ctrip.hermes.portal.dal;

import java.util.List;
import java.util.Map;

import org.unidal.dal.jdbc.DalException;
import org.unidal.tuple.Pair;

public interface HermesPortalDao {
	
	public MessagePriority getLatestProduced(String topic, int partition, int priority) throws DalException;

	/**
	 * 
	 * @param topic
	 * @param partition
	 * @return key=offsetMsg.GroupId, value.key=offsetMsg(priority), value.value=offsetMsg(non-priority).
	 * @throws DalException
	 */
	public Map<Integer, Pair<OffsetMessage, OffsetMessage>> getLatestConsumed(String topic, int partition) throws DalException;

	public List<MessagePriority> getLatestMessages(String topic, int pratition, int count) throws DalException;

	MessagePriority getMsgById(String topic, int partition, int priority, long id) throws DalException;
	
}
