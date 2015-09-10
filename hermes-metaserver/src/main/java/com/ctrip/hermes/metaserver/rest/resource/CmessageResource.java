package com.ctrip.hermes.metaserver.rest.resource;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.curator.utils.EnsurePath;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaserver.commons.BaseZKWatcher;
import com.ctrip.hermes.metaservice.zk.ZKClient;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;
import com.ctrip.hermes.metaservice.zk.ZKSerializeUtils;

@Path("/cmessage/exchange")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class CmessageResource {
	private static final Logger log = LoggerFactory.getLogger(CmessageResource.class);

	private AtomicReference<String> m_info = new AtomicReference<String>("");

	private ZKClient m_zkClient = PlexusComponentLocator.lookup(ZKClient.class);

	public CmessageResource() {
		updateExchangeInfo();
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

	@GET
	public Response getExchangeList() {
		String str = m_info.get();
		if (str == null || str.length() == 0) {
			str = "[]";
		}
		return Response.status(Status.OK).entity(str).build();
	}
}
