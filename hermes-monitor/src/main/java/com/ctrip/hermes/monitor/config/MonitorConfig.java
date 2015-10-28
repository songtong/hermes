package com.ctrip.hermes.monitor.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource(value = "classpath:hermes.properties")
public class MonitorConfig {

	@Value("${es.cluster.name}")
	private String esClusterName;

	@Value("${es.transport.address}")
	private String[] esTransportAddress;

	public String getEsClusterName() {
		return esClusterName;
	}

	public void setEsClusterName(String esClusterName) {
		this.esClusterName = esClusterName;
	}

	public String[] getEsTransportAddress() {
		return esTransportAddress;
	}

	public void setEsTransportAddress(String[] esTransportAddress) {
		this.esTransportAddress = esTransportAddress;
	}
}
