package com.ctrip.hermes.metaserver.commons;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.ctrip.hermes.metaserver.commons.ActiveClientList.ClientContext;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface ActiveClientListHolder<Key> {

	public void heartbeat(Key key, String clientName, String ip, int port);

	public ActiveClientList getActiveClientList(Key key);

	public Map<Key, Map<String, ClientContext>> scanChanges(long timeout, TimeUnit timeUnit);

}