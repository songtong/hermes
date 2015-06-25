package com.ctrip.hermes.metaserver.event.impl;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.metaserver.commons.BaseEventBasedZkWatcher;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;
import com.ctrip.hermes.metaserver.event.Event;
import com.ctrip.hermes.metaserver.event.EventEngineContext;
import com.ctrip.hermes.metaserver.event.EventHandler;
import com.ctrip.hermes.metaserver.event.EventType;
import com.ctrip.hermes.metaserver.meta.MetaHolder;
import com.ctrip.hermes.metaserver.meta.MetaInfo;
import com.ctrip.hermes.metaservice.zk.ZKClient;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;
import com.ctrip.hermes.metaservice.zk.ZKSerializeUtils;
import com.google.common.io.ByteStreams;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = EventHandler.class, value = "FollowerInitEventHandler")
public class FollowerInitEventHandler extends BaseEventHandler implements Initializable {

	private static final Logger log = LoggerFactory.getLogger(FollowerInitEventHandler.class);

	@Inject
	private MetaServerConfig m_config;

	@Inject
	private MetaHolder m_metaHolder;

	@Inject
	private ZKClient m_zkClient;

	@Override
	public EventType eventType() {
		return EventType.FOLLOWER_INIT;
	}

	@Override
	public void initialize() throws InitializationException {
	}

	@Override
	protected void processEvent(EventEngineContext context, Event event) throws Exception {
		loadAndaddMetaInfoWatcher(new LeaderMetaUpdateWatcher(context));
	}

	private void loadAndaddMetaInfoWatcher(Watcher watcher) throws Exception {
		byte[] data = m_zkClient.getClient().getData().usingWatcher(watcher).forPath(ZKPathUtils.getMetaInfoZkPath());
		MetaInfo metaInfo = ZKSerializeUtils.deserialize(data, MetaInfo.class);
		Meta meta = fetchMetaInfo(metaInfo);
		if (meta != null) {
			m_metaHolder.setMeta(meta);
			log.info("Fetched meta from leader(endpoint={}:{},version={})", metaInfo.getHost(), metaInfo.getPort(),
			      meta.getVersion());
		}
	}

	private Meta fetchMetaInfo(MetaInfo metaInfo) {
		try {
			String url = null;
			Meta meta = m_metaHolder.getMeta();
			if (meta != null) {
				url = "http://" + metaInfo.getHost() + ":" + metaInfo.getPort() + "/meta?version=" + meta.getVersion();
			} else {
				url = "http://" + metaInfo.getHost() + ":" + metaInfo.getPort() + "/meta";
			}
			URL metaURL = new URL(url);
			HttpURLConnection connection = (HttpURLConnection) metaURL.openConnection();
			connection.setConnectTimeout(m_config.getFetcheMetaFromLeaderConnectTimeout());
			connection.setReadTimeout(m_config.getFetcheMetaFromLeaderReadTimeout());
			connection.setRequestMethod("GET");
			connection.connect();
			if (connection.getResponseCode() == 200) {
				InputStream is = connection.getInputStream();
				String jsonString = new String(ByteStreams.toByteArray(is));
				return JSON.parseObject(jsonString, Meta.class);
			} else if (connection.getResponseCode() == 304) {
				return null;
			}
		} catch (Exception e) {
			log.error("Failed to fetch meta from leader({}:{})", metaInfo.getHost(), metaInfo.getPort(), e);
		}
		return null;
	}

	@Override
	protected Role role() {
		return Role.FOLLOWER;
	}

	private class LeaderMetaUpdateWatcher extends BaseEventBasedZkWatcher {

		protected LeaderMetaUpdateWatcher(EventEngineContext context) {
			super(context.getEventBus(), context.getWatcherExecutor(), context.getClusterStateHolder(),
			      org.apache.zookeeper.Watcher.Event.EventType.NodeDataChanged);
		}

		@Override
		protected void doProcess(WatchedEvent event) {
			try {
				loadAndaddMetaInfoWatcher(this);
			} catch (Exception e) {
				log.error("Exception occurred while handling leader meta watcher event.", e);
			}
		}

	}

}