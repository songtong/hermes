package com.ctrip.hermes.core.status;

import java.util.concurrent.BlockingQueue;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.metrics.HermesMetricsRegistry;
import com.dianping.cat.Cat;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public enum StatusMonitor {
	INSTANCE;

	private StatusMonitor() {

	}

	public void addCommandProcessorThreadPoolGauge(String threadPoolNamePrefix, final BlockingQueue<Runnable> workQueue) {
		HermesMetricsRegistry.getMetricRegistry().register(MetricRegistry.name(threadPoolNamePrefix, "queue", "size"),
		      new Gauge<Integer>() {

			      @Override
			      public Integer getValue() {
				      return workQueue.size();
			      }
		      });
	}

	public void commandReceived(CommandType type, String clientIp) {
		HermesMetricsRegistry.getMetricRegistry().meter(MetricRegistry.name("commandProcessor", "commandReceived"))
		      .mark();
		HermesMetricsRegistry.getMetricRegistry()
		      .meter(MetricRegistry.name("commandProcessor", "commandReceived", clientIp)).mark();
		if (type != null) {
			Cat.logEvent("Hermes.Command.Version", type.toString() + "-RCV");
		}
	}

	public Timer getProcessCommandTimer(CommandType type, CommandProcessor processor) {
		return HermesMetricsRegistry.getMetricRegistry().timer(
		      MetricRegistry.name(type.name(), processor.getClass().getName(), "duration"));
	}

	public void commandProcessorException(CommandType type, CommandProcessor processor) {
		HermesMetricsRegistry.getMetricRegistry().meter(MetricRegistry.name("commandProcessor", "exception")).mark();
	}

}
