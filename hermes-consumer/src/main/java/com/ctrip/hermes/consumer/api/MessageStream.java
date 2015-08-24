package com.ctrip.hermes.consumer.api;

import java.util.List;

import com.ctrip.hermes.core.message.ConsumerMessage;

public interface MessageStream<T> {
	/**
	 * 
	 * @return partition id of this MessageStream
	 */
	int getParatitionId();

	/**
	 * 
	 * @param offset
	 *           start offset of messages to fetch, included
	 * @param size
	 *           how many messages to fetch
	 * @return messages from offset
	 */
	List<ConsumerMessage<T>> fetchMessages(long offset, int size);

	/**
	 * 
	 * @param offsets
	 *           message offsets to fetch
	 * @return messages whose offset is in offsets
	 */
	List<ConsumerMessage<T>> fetchMessages(List<Long> offsets);

	/**
	 * 
	 * @param time
	 *           message born time to query
	 * @return The offset of message whose born time is closest to time.<br/>
	 *         If time == Long.MIN_VALUE, then return the offset of the oldest message.<br/>
	 *         If time == Long.MAX_VALUE, then return the offset of the newest message.
	 */
	long getOffsetByTime(long time); // Long.MIN_VALUE返回最早的offset，Long.MAX_VALUE返回最新的offset
}
