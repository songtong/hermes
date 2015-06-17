package com.ctrip.hermes.metaserver.broker.watcher;

import java.util.concurrent.ExecutorService;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;

import com.ctrip.hermes.metaserver.commons.BaseZkWatcher;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class LeaseWatcher extends BaseZkWatcher {

	public LeaseWatcher(ExecutorService executorService) {
		super(executorService, EventType.NodeDataChanged, EventType.NodeDeleted);
	}

	@Override
	protected void doProcess(WatchedEvent event) {
		// TODO Auto-generated method stub

	}

}
