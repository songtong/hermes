package com.ctrip.hermes.core.config;

import org.unidal.lookup.annotation.Named;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = CoreConfig.class)
public class CoreConfig {

	public int getCommandProcessorThreadCount() {
		return 10;
	}

	public int getMetaServerIpFetchInterval() {
		return 5;
	}

	public int getMetaServerConnectTimeout() {
		return 1000;
	}

	public int getMetaServerReadTimeout() {
		return 2000;
	}

	public long getRunningStatusStatInterval() {
		return 30;
	}

	public long getMetaCacheRefreshIntervalSeconds() {
		return 1;
	}

	public int getNettySendBufferSize() {
		return 65535;
	}

	public int getNettyReceiveBufferSize() {
		return 65535;
	}

	public int getEndpointChannelSendBufferSize() {
		return 1000;
	}

	public int getEndpointChannelWriterCheckIntervalBase() {
		return 5;
	}
	
	public int getEndpointChannelWriterCheckIntervalMax() {
		return 50;
	}

	public long getEndpointChannelWriteRetryDelay() {
		return 20;
	}

	public long getEndpointChannelAutoReconnectDelay() {
		return 1;
	}

	public long getEndpointChannelDefaultWrtieTimeout() {
		return 3600 * 1000L;
	}

	public int getEndpointChannelMaxIdleTime() {
		return 60;
	}

	public int getEndpointChannelReadIdleTime() {
		return 30;
	}

	public int getEndpointChannelWriteIdleTime() {
		return 30;
	}

	public String getAvroSchemaRetryUrlKey() {
		return "schema.registry.url";
	}

}
