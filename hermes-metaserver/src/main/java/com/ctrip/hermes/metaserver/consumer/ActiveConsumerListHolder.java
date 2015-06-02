package com.ctrip.hermes.metaserver.consumer;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.unidal.tuple.Pair;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface ActiveConsumerListHolder {

	public void heartbeat(String topicName, String consumerGroupName, String consumerName);

	public ActiveConsumerList getActiveConsumerList(String topicName, String consumerGroupName);

	public Map<Pair<String, String>, Set<String>> scanChanges(long timeout, TimeUnit timeUnit);

}