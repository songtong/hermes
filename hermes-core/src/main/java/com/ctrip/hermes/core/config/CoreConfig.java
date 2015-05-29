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
		return 60;
	}

	public int getMetaServerConnectTimeout() {
		return 2000;
	}

	public int getMetaServerReadTimeout() {
		return 5000;
	}

	public long getRunningStatusStatInterval() {
		return 30;
	}

	public long getMetaCacheRefreshIntervalMinutes() {
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

	public long getEndpointChannelWriterCheckInterval() {
		return 20;
	}

	public long getEndpointChannelWriteRetryDealy() {
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

	public String getAvroSchemaRetryUrlKey() {
		return "schema.registry.url";
	}

}
