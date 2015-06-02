package com.ctrip.hermes.metaserver.commons;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface ActiveClientListHolder<Key> {

	public void heartbeat(Key key, String clientName);

	public ActiveClientList getActiveClientList(Key key);

	public Map<Key, Set<String>> scanChanges(long timeout, TimeUnit timeUnit);

}