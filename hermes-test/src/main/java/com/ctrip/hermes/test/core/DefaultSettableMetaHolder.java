package com.ctrip.hermes.test.core;

import java.util.concurrent.atomic.AtomicReference;

import com.ctrip.hermes.meta.entity.Meta;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class DefaultSettableMetaHolder implements SettableMetaHolder {

	private AtomicReference<Meta> m_meta = new AtomicReference<>();

	@Override
	public void setMeta(Meta meta) {
		m_meta.set(meta);
	}

	@Override
	public Meta getMeta() {
		return m_meta.get();
	}

}
