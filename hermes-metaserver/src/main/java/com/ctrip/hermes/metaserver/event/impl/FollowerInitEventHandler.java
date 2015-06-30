package com.ctrip.hermes.metaserver.event.impl;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.fluent.Request;
import org.apache.http.util.EntityUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.net.Networks;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.metaserver.broker.BrokerAssignmentHolder;
import com.ctrip.hermes.metaserver.commons.BaseEventBasedZkWatcher;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;
import com.ctrip.hermes.metaserver.event.Event;
import com.ctrip.hermes.metaserver.event.EventEngineContext;
import com.ctrip.hermes.metaserver.event.EventHandler;
import com.ctrip.hermes.metaserver.event.EventType;
import com.ctrip.hermes.metaserver.meta.MetaHolder;
import com.ctrip.hermes.metaserver.meta.MetaInfo;
import com.ctrip.hermes.metaserver.meta.MetaServerAssignmentHolder;
import com.ctrip.hermes.metaservice.zk.ZKClient;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;
import com.ctrip.hermes.metaservice.zk.ZKSerializeUtils;

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
	private BrokerAssignmentHolder m_brokerAssignmentHolder;

	@Inject
	private MetaServerAssignmentHolder m_metaServerAssignmentHolder;

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
		m_brokerAssignmentHolder.clear();
		loadAndaddMetaInfoWatcher(new LeaderMetaChangedWatcher(context));

		loadAndAddMetaServerAssignmentWatcher(new MetaServerAssignmentChangedWatcher(context));
	}

	private void loadAndAddMetaServerAssignmentWatcher(MetaServerAssignmentChangedWatcher watcher) throws Exception {
		m_zkClient.getClient().getData().usingWatcher(watcher).forPath(ZKPathUtils.getMetaServerAssignmentRootZkPath());
		m_metaServerAssignmentHolder.reload();
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
		if(metaInfo == null){
			return null;
		}

		try {
			if(Networks.forIp().getLocalHostAddress().equals(metaInfo.getHost()) && m_config.getMetaServerPort() == metaInfo
					  .getPort()){
				return null;
			}

			String url = String.format("http://%s:%s/meta", metaInfo.getHost(), metaInfo.getPort());
			Meta meta = m_metaHolder.getMeta();

			if (meta != null) {
				url += "?version=" + meta.getVersion();
			}

			HttpResponse response = Request.Get(url)//
			      .connectTimeout(m_config.getFetcheMetaFromLeaderConnectTimeout())//
			      .socketTimeout(m_config.getFetcheMetaFromLeaderReadTimeout())//
			      .execute()//
			      .returnResponse();

			if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
				String responseContent = EntityUtils.toString(response.getEntity());
				return JSON.parseObject(responseContent, Meta.class);
			} else if (response.getStatusLine().getStatusCode() == HttpStatus.SC_NOT_MODIFIED) {
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

	private class LeaderMetaChangedWatcher extends BaseEventBasedZkWatcher {

		protected LeaderMetaChangedWatcher(EventEngineContext context) {
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

	private class MetaServerAssignmentChangedWatcher extends BaseEventBasedZkWatcher {

		protected MetaServerAssignmentChangedWatcher(EventEngineContext context) {
			super(context.getEventBus(), context.getWatcherExecutor(), context.getClusterStateHolder(),
			      org.apache.zookeeper.Watcher.Event.EventType.NodeDataChanged);
		}

		@Override
		protected void doProcess(WatchedEvent event) {
			try {
				loadAndAddMetaServerAssignmentWatcher(this);
			} catch (Exception e) {
				log.error("Exception occurred while handling meta server assignment watcher event.", e);
			}
		}

	}
}
