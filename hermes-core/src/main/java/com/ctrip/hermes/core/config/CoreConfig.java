package com.ctrip.hermes.core.config;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.utils.StringUtils;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = CoreConfig.class)
public class CoreConfig implements Initializable {

	private static final int DEFAULT_CHANNEL_READ_IDLE_TIME_SECONDS = 30;

	private static final int DEFAULT_CHANNEL_WRITE_IDLE_TIME_SECONDS = 30;

	private static final int DEFAULT_CHANNEL_ALL_IDLE_TIME_SECONDS = 60;

	@Inject
	private ClientEnvironment m_env;

	private int m_channelReadIdle = DEFAULT_CHANNEL_READ_IDLE_TIME_SECONDS;

	private int m_channelWriteIdle = DEFAULT_CHANNEL_WRITE_IDLE_TIME_SECONDS;

	private int m_channelAllIdle = DEFAULT_CHANNEL_ALL_IDLE_TIME_SECONDS;

	@Override
	public void initialize() throws InitializationException {
		String readIdleStr = m_env.getGlobalConfig().getProperty("channel.read.idle.seconds");
		if (StringUtils.isNumeric(readIdleStr)) {
			m_channelReadIdle = Integer.valueOf(readIdleStr);
		}
		String writeIdle = m_env.getGlobalConfig().getProperty("channel.write.idle.seconds");
		if (StringUtils.isNumeric(writeIdle)) {
			m_channelWriteIdle = Integer.valueOf(writeIdle);
		}

		String allIdleStr = m_env.getGlobalConfig().getProperty("channel.all.idle.seconds");
		if (StringUtils.isNumeric(allIdleStr)) {
			m_channelAllIdle = Integer.valueOf(allIdleStr);
		}
	}

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
		return m_channelAllIdle;
	}

	public int getEndpointChannelReadIdleTime() {
		return m_channelReadIdle;
	}

	public int getEndpointChannelWriteIdleTime() {
		return m_channelWriteIdle;
	}

	public String getAvroSchemaRetryUrlKey() {
		return "schema.registry.url";
	}

}
