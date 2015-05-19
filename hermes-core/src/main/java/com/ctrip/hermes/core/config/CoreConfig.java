package com.ctrip.hermes.core.config;

import org.unidal.lookup.annotation.Named;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = CoreConfig.class)
public class CoreConfig {

	public String getBackgroundThreadGroup() {
		return "Hermes-Core-Background";
	}

	public int getCommandProcessorThreadCount() {
		return 10;
	}
}
