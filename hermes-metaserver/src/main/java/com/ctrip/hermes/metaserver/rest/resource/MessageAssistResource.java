package com.ctrip.hermes.metaserver.rest.resource;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.bo.HostPort;
import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.schedule.ExponentialSchedulePolicy;
import com.ctrip.hermes.core.transport.command.QueryMessageOffsetByTimeCommand;
import com.ctrip.hermes.core.transport.command.QueryOffsetResultCommand;
import com.ctrip.hermes.core.transport.endpoint.EndpointClient;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaserver.broker.BrokerAssignmentHolder;
import com.ctrip.hermes.metaserver.broker.endpoint.MetaEndpointClient;
import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;
import com.ctrip.hermes.metaserver.commons.Assignment;
import com.ctrip.hermes.metaserver.commons.ClientContext;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;
import com.ctrip.hermes.metaserver.meta.MetaHolder;
import com.ctrip.hermes.metaserver.monitor.QueryOffsetResultMonitor;
import com.ctrip.hermes.metaserver.rest.commons.RestException;
import com.google.common.util.concurrent.SettableFuture;

@Path("/message/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class MessageAssistResource {
	private static final Logger log = LoggerFactory.getLogger(MessageAssistResource.class);

	private BrokerAssignmentHolder m_brokerAssignments = PlexusComponentLocator.lookup(BrokerAssignmentHolder.class);

	private MetaHolder m_metaHolder = PlexusComponentLocator.lookup(MetaHolder.class);

	private EndpointClient m_endpointClient = PlexusComponentLocator.lookup(EndpointClient.class, MetaEndpointClient.ID);

	private MetaServerConfig m_config = PlexusComponentLocator.lookup(MetaServerConfig.class);

	private ExecutorService m_offsetQueryExecutor = Executors.newFixedThreadPool(3);

	private ClusterStateHolder m_clusterStateHolder = PlexusComponentLocator.lookup(ClusterStateHolder.class);

	private QueryOffsetResultMonitor m_monitor = PlexusComponentLocator.lookup(QueryOffsetResultMonitor.class);

	@GET
	@Path("offset")
	public Response queryMessageIdByTime( //
	      @QueryParam("topic") String topic, //
	      @QueryParam("partition") @DefaultValue("-1") int partition, //
	      @QueryParam("time") @DefaultValue("-1") long time) {
		time = -1 == time ? Long.MAX_VALUE : -2 == time ? Long.MIN_VALUE : time;

		Map<Integer, Offset> result;
		if (m_clusterStateHolder.hasLeadership()) {
			result = findOffsetFromBroker(topic, partition, time);
		} else {
			Map<String, String> params = new HashMap<String, String>();
			params.put("topic", topic);
			params.put("partition", String.valueOf(partition));
			params.put("time", String.valueOf(time));

			HostPort leader = m_clusterStateHolder.getLeader();
			result = findOffsetFromMetaLeader(leader.getHost(), leader.getPort(), params);
		}

		if (result == null || result.size() == 0) {
			return Response.status(Status.NOT_FOUND)
			      .entity(String.format("No offset found for [%s, %s, %s]", topic, partition, time)).build();
		}

		return Response.status(Status.OK).entity(result).build();
	}

	private Map<Integer, Offset> findOffsetFromBroker(final String topicName, final int partition, final long time) {
		final Map<Integer, Offset> result = new ConcurrentHashMap<Integer, Offset>();
		Topic topic = m_metaHolder.getMeta().findTopic(topicName);
		if (topic != null) {
			try {
				final Assignment<Integer> assignment = m_brokerAssignments.getAssignment(topicName);

				if (assignment == null) {
					return null;
				}

				if (partition >= 0 && topic.findPartition(partition) != null) {
					Map<String, ClientContext> partitionAssignment = assignment.getAssignment(partition);
					if (partitionAssignment != null && partitionAssignment.size() > 0) {
						ClientContext context = partitionAssignment.entrySet().iterator().next().getValue();
						result.put(partition, findOffsetByTime(topicName, partition, time, getBrokerEndpoint(context)));
					}
				} else if (partition == -1) {
					final CountDownLatch latch = new CountDownLatch(topic.getPartitions().size());
					for (Partition p : topic.getPartitions()) {
						final int id = p.getId();
						m_offsetQueryExecutor.execute(new Runnable() {
							@Override
							public void run() {
								try {
									Map<String, ClientContext> partitionAssignment = assignment.getAssignment(id);
									if (partitionAssignment != null && partitionAssignment.size() > 0) {
										ClientContext client = partitionAssignment.entrySet().iterator().next().getValue();
										result.put(id, findOffsetByTime(topicName, id, time, getBrokerEndpoint(client)));
									}
								} catch (Exception e) {
									log.error("Query message offset failed: {}:{} {}", topicName, partition, time, e);
									result.put(id, null);
								} finally {
									latch.countDown();
								}
							}
						});
					}
					latch.await(m_config.getQueryMessageOffsetTimeoutMillis(), TimeUnit.MILLISECONDS);
				}
				return result;
			} catch (Exception e) {
				log.error("Query message offset failed: {}:{} {}", topicName, partition, time, e);
				throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
			}
		} else {
			throw new RestException(String.format("Topic %s not found.", topicName), Status.NOT_FOUND);
		}
	}

	private Endpoint getBrokerEndpoint(ClientContext context) {
		return new Endpoint()//
		      .setId(context.getName()) //
		      .setType(Endpoint.BROKER) //
		      .setHost(context.getIp()) //
		      .setPort(context.getPort());
	}

	private Offset findOffsetByTime(String topic, int partition, long time, Endpoint endpoint) throws Exception {
		long timeout = m_config.getQueryMessageOffsetTimeoutMillis();
		ExponentialSchedulePolicy backoff = new ExponentialSchedulePolicy(500, (int) timeout);

		long expire = System.currentTimeMillis() + timeout;
		while (!Thread.interrupted() && System.currentTimeMillis() < expire) {
			SettableFuture<QueryOffsetResultCommand> future = SettableFuture.create();
			QueryMessageOffsetByTimeCommand cmd = new QueryMessageOffsetByTimeCommand(topic, partition, time);
			cmd.setFuture(future);

			m_monitor.monitor(cmd);
			if (m_endpointClient.writeCommand(endpoint, cmd, timeout, TimeUnit.MILLISECONDS)) {
				QueryOffsetResultCommand resultCmd = null;
				try {
					resultCmd = future.get(timeout, TimeUnit.MILLISECONDS);
					if (resultCmd != null && resultCmd.getOffset() != null) {
						return resultCmd.getOffset();
					} else {
						log.debug("Find offset[topic:{}({})] from broker[{}] failed, will retry!", topic, partition, endpoint);
						backoff.fail(true);
					}
				} finally {
					m_monitor.remove(cmd);
				}
			} else {
				m_monitor.remove(cmd);
			}
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	private Map<Integer, Offset> findOffsetFromMetaLeader(String host, int port, Map<String, String> params) {
		if (log.isDebugEnabled()) {
			log.debug("Proxy pass find-offset request to {}:{} (params={})", host, port, params);
		}
		try {
			URIBuilder uriBuilder = new URIBuilder()//
			      .setScheme("http")//
			      .setHost(host)//
			      .setPort(port)//
			      .setPath("/message/offset");

			if (params != null) {
				for (Map.Entry<String, String> entry : params.entrySet()) {
					uriBuilder.addParameter(entry.getKey(), entry.getValue());
				}
			}

			HttpResponse response = Request.Get(uriBuilder.build())//
			      .connectTimeout(m_config.getProxyPassConnectTimeout())//
			      .socketTimeout(m_config.getProxyPassReadTimeout())//
			      .execute()//
			      .returnResponse();

			if (response != null && response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
				String responseContent = EntityUtils.toString(response.getEntity());
				if (!StringUtils.isBlank(responseContent)) {
					return (Map<Integer, Offset>) JSON.parse(responseContent);
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("Response error while proxy passing to {}:{}(status={}}).", host, port, response
					      .getStatusLine().getStatusCode());
				}
			}
		} catch (Exception e) {
			if (log.isDebugEnabled()) {
				log.debug("Failed to proxy pass to http://{}:{}.", host, port, e);
			}
		}
		return null;
	}
}
