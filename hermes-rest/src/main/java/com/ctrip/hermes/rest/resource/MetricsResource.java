package com.ctrip.hermes.rest.resource;

import java.util.Collection;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.ctrip.hermes.rest.service.SubscriptionPushCommand;
import com.ctrip.hermes.rest.service.ProducerSendCommand;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixCommandMetrics;

@Path("/metrics/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class MetricsResource {

	@Path("topics")
	@GET
	public HystrixCommandMetrics getTopicMetrics() {
		HystrixCommandMetrics topicMetrics = HystrixCommandMetrics.getInstance(HystrixCommandKey.Factory
		      .asKey(ProducerSendCommand.class.getSimpleName()));
		return topicMetrics;
	}

	@Path("subscriptions")
	@GET
	public HystrixCommandMetrics getSubscriptionMetrics() {
		HystrixCommandMetrics subMetrics = HystrixCommandMetrics.getInstance(HystrixCommandKey.Factory
		      .asKey(SubscriptionPushCommand.class.getSimpleName()));
		return subMetrics;
	}

	@Path("all")
	@GET
	public Collection<HystrixCommandMetrics> getHystrixMetrics() {
		Collection<HystrixCommandMetrics> metrics = HystrixCommandMetrics.getInstances();
		return metrics;
	}
}
