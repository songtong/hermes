package com.ctrip.hermes.collector.hub;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.collector.conf.CollectorConfiguration;
import com.ctrip.hermes.collector.utils.CollectorThreadFactory;
import com.ctrip.hickwall.proxy.HickwallClient;
import com.ctrip.hickwall.proxy.common.DataPoint;

@Component
public class MetricsHub {
	private static final Logger LOGGER = LoggerFactory.getLogger(MetricsHub.class);
	@Autowired
	private CollectorConfiguration m_conf;
	
	private ExecutorService m_service = Executors.newSingleThreadExecutor(CollectorThreadFactory.newFactory(MetricsHub.class));
	
	private HickwallClient m_client;
	
	@PostConstruct
	private void init() {
		try {
			m_client = new HickwallClient(m_conf.getHickwallServers());
		} catch (IOException e) {
			LOGGER.error("Failed to create hickwall client: {}", e.getMessage(), e);
		}
	}
	
	public void send(final ArrayList<DataPoint> points) {
		if (m_client == null) {
			return;
		}
		
		Future<?> future = m_service.submit(new Runnable() {

			@Override
			public void run() {
				try {
					m_client.send(points);
				} catch (IOException e) {
					LOGGER.error("Failed to send metric points: {}", e.getMessage(), e);
				}
			}
			
		});
		
		try {
			future.get(m_conf.getHickwallSendTimeout(), TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			LOGGER.error("Failed to send metric points: {}", e.getMessage(), e);
		}
	}
}
