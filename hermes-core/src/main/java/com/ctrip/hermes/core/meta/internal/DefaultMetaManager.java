package com.ctrip.hermes.core.meta.internal;

import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.meta.MetaManager;
import com.ctrip.hermes.core.meta.MetaProxy;
import com.ctrip.hermes.core.meta.remote.RemoteMetaProxy;
import com.ctrip.hermes.meta.entity.Meta;

@Named(type = MetaManager.class)
public class DefaultMetaManager extends ContainerHolder implements MetaManager {

	@Inject(LocalMetaLoader.ID)
	private MetaLoader m_localMeta;

	@Inject(RemoteMetaLoader.ID)
	private MetaLoader m_remoteMeta;

	@Inject
	private ClientEnvironment m_env;

	@Inject(LocalMetaProxy.ID)
	private MetaProxy m_localMetaProxy;

	@Inject(RemoteMetaProxy.ID)
	private MetaProxy m_remoteMetaProxy;

	@Override
	public MetaProxy getMetaProxy() {
		if (isLocalMode()) {
			return m_localMetaProxy;
		} else {
			return m_remoteMetaProxy;
		}
	}

	@Override
	public Meta getMeta() {
		if (isLocalMode()) {
			return m_localMeta.load();
		} else {
			return m_remoteMeta.load();
		}
	}

	private boolean isLocalMode() {
		if (System.getenv().containsKey("isLocalMode")) {
			return Boolean.parseBoolean(System.getenv("isLocalMode"));
		} else if (m_env.getGlobalConfig().containsKey("isLocalMode")) {
			return Boolean.parseBoolean(m_env.getGlobalConfig().getProperty("isLocalMode"));
		}
		// FIXME for dev only
		return true;
	}

}
