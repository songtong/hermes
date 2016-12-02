package com.ctrip.hermes.core.meta.manual;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface ManualConfigFetcher {
	public ManualConfig fetchConfig(ManualConfig oldConfig);
}
