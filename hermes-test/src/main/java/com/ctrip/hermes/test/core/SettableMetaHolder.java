package com.ctrip.hermes.test.core;

import com.ctrip.hermes.meta.entity.Meta;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface SettableMetaHolder {
	public void setMeta(Meta meta);

	public Meta getMeta();
}
