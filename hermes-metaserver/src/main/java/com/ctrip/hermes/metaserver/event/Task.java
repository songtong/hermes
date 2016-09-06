package com.ctrip.hermes.metaserver.event;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface Task {
	public void run() throws Exception;

	public String name();
}
