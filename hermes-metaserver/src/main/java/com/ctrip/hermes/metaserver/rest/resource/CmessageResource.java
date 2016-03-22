package com.ctrip.hermes.metaserver.rest.resource;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.curator.utils.EnsurePath;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.cmessaging.entity.Cmessaging;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaserver.commons.BaseZKWatcher;
import com.ctrip.hermes.metaservice.zk.ZKClient;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;
import com.ctrip.hermes.metaservice.zk.ZKSerializeUtils;

@Path("/cmessage/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class CmessageResource {
	private static final Logger log = LoggerFactory.getLogger(CmessageResource.class);

	private AtomicReference<String> m_info = new AtomicReference<String>("");

	private AtomicReference<String> m_cmsgConfig = new AtomicReference<String>("");

	private AtomicLong m_cmsgConfigVersion = new AtomicLong(0);

	private ZKClient m_zkClient = PlexusComponentLocator.lookup(ZKClient.class);

	public CmessageResource() {
		updateExchangeInfo();
		updateCmsgConfig();
	}

	private void updateExchangeInfo() {
		try {
			EnsurePath ensurePath = m_zkClient.get().newNamespaceAwareEnsurePath(ZKPathUtils.getCmessageExchangePath());
			ensurePath.ensure(m_zkClient.get().getZookeeperClient());

			ExchangeWatcher watcher = new ExchangeWatcher(Executors.newSingleThreadExecutor(), EventType.NodeDataChanged);
			m_info.set(ZKSerializeUtils.deserialize(
			      m_zkClient.get().getData().usingWatcher(watcher).forPath(ZKPathUtils.getCmessageExchangePath()),
			      String.class));
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Query cmessage exchange info failed.", e);
		}
	}

	private class ExchangeWatcher extends BaseZKWatcher {

		protected ExchangeWatcher(ExecutorService executor, EventType acceptedEventTypes) {
			super(executor, acceptedEventTypes);
		}

		@Override
		protected void doProcess(WatchedEvent event) {
			updateExchangeInfo();
		}
	}

	private void updateCmsgConfig() {
		try {
			EnsurePath ensurePath = m_zkClient.get().newNamespaceAwareEnsurePath(ZKPathUtils.getCmessageConfigPath());
			ensurePath.ensure(m_zkClient.get().getZookeeperClient());

			CmsgConfigWatcher watcher = new CmsgConfigWatcher(Executors.newSingleThreadExecutor(),
			      EventType.NodeDataChanged);
			String configString = ZKSerializeUtils.deserialize(
			      m_zkClient.get().getData().usingWatcher(watcher).forPath(ZKPathUtils.getCmessageConfigPath()),
			      String.class);
			m_cmsgConfig.set(configString);
			m_cmsgConfigVersion.set(JSON.parseObject(configString, Cmessaging.class).getVersion());
		} catch (Exception e) {
			log.error("Query cmessage config failed.", e);
		}
	}

	private class CmsgConfigWatcher extends BaseZKWatcher {

		protected CmsgConfigWatcher(ExecutorService executor, EventType acceptedEventTypes) {
			super(executor, acceptedEventTypes);
		}

		@Override
		protected void doProcess(WatchedEvent event) {
			updateCmsgConfig();
		}
	}

	@GET
	@Path("exchange")
	public Response getExchangeList() {
		String str = m_info.get();
		if (str == null || str.length() == 0) {
			str = "[]";
		}
		return Response.status(Status.OK).entity(str).build();
	}

	@GET
	@Path("config")
	public Response getConfig(@QueryParam("version") @DefaultValue("0") long version) {
		if (version > 0 && version == m_cmsgConfigVersion.get()) {
			return Response.status(Status.NOT_MODIFIED).build();
		} else {
			return Response.status(Status.OK).entity(m_cmsgConfig.get()).build();
		}
	}
}
