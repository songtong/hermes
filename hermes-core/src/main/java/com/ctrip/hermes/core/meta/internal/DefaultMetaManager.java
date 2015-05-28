package com.ctrip.hermes.core.meta.internal;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.meta.remote.RemoteMetaLoader;
import com.ctrip.hermes.core.meta.remote.RemoteMetaProxy;
import com.ctrip.hermes.meta.entity.Meta;

@Named(type = MetaManager.class)
public class DefaultMetaManager extends ContainerHolder implements MetaManager, Initializable {
	private static final String KEY_IS_LOCAL_MODE = "isLocalMode";

	private static final Logger log = LoggerFactory.getLogger(DefaultMetaManager.class);

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

	private boolean m_localMode = false;

	@Override
	public MetaProxy getMetaProxy() {
		if (isLocalMode()) {
			return m_localMetaProxy;
		} else {
			return m_remoteMetaProxy;
		}
	}

	@Override
	public Meta loadMeta() {
		if (isLocalMode()) {
			return m_localMeta.load();
		} else {
			return m_remoteMeta.load();
		}
	}

	private boolean isLocalMode() {
		return m_localMode;
	}

	@Override
	public void initialize() throws InitializationException {
		if (System.getenv().containsKey(KEY_IS_LOCAL_MODE)) {
			m_localMode = Boolean.parseBoolean(System.getenv(KEY_IS_LOCAL_MODE));
		} else if (m_env.getGlobalConfig().containsKey(KEY_IS_LOCAL_MODE)) {
			m_localMode = Boolean.parseBoolean(m_env.getGlobalConfig().getProperty(KEY_IS_LOCAL_MODE));
		} else {
			m_localMode = false;
		}

		if (m_localMode) {
			log.info("Meta manager started with local mode");
		} else {
			log.info("Meta manager started with remote mode");
		}
	}
}
