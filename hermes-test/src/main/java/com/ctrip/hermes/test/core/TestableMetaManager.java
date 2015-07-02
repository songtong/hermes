package com.ctrip.hermes.test.core;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.core.meta.internal.MetaManager;
import com.ctrip.hermes.core.meta.internal.MetaProxy;
import com.ctrip.hermes.meta.entity.Meta;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class TestableMetaManager implements MetaManager {

	@Inject
	private SettableMetaHolder m_metaHolder;

	@Inject
	private MetaProxy m_metaProxy;

	@Override
	public Meta loadMeta() {
		return m_metaHolder.getMeta();
	}

	@Override
	public MetaProxy getMetaProxy() {
		return m_metaProxy;
	}

}
