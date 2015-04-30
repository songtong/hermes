package com.ctrip.hermes.core.meta.internal;

import java.io.IOException;

import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.meta.MetaManager;
import com.ctrip.hermes.meta.entity.Meta;

@Named(type = MetaManager.class, value = ClientMetaManager.ID)
public class ClientMetaManager extends ContainerHolder implements MetaManager {

	public static final String ID = "meta-client";

	@Inject(LocalMetaLoader.ID)
	private MetaLoader m_localMeta;

	@Inject(RemoteMetaLoader.ID)
	private MetaLoader m_remoteMeta;

	@Inject
	private ClientEnvironment m_env;

	@Override
	public Meta getMeta(boolean isForceLatest) {
		if (isLocalMode()) {
			return m_localMeta.load();
		} else {
			return m_remoteMeta.load();
		}
	}

	@Override
	public Meta getMeta() {
		return getMeta(false);
	}

	private boolean isLocalMode() {
		if (System.getenv().containsKey("isLocalMode")) {
			return Boolean.parseBoolean(System.getenv("isLocalMode"));
		} else
			try {
				if (m_env.getGlobalConfig().containsKey("isLocalMode")) {
					return Boolean.parseBoolean(m_env.getGlobalConfig().getProperty("isLocalMode"));
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		// FIXME for dev only
		return true;
	}

	@Override
	public boolean updateMeta(Meta meta) {
		if (isLocalMode()) {
			return m_localMeta.save(meta);
		} else {
			return m_remoteMeta.save(meta);
		}
	}

}
