package com.ctrip.hermes.metaserver.config;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.net.Networks;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.bo.HostPort;
import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.utils.StringUtils;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = MetaServerConfig.class)
public class MetaServerConfig {

	@Inject
	private ClientEnvironment m_env;

	public long getDefaultLeaseAcquireOrRenewRetryDelayMillis() {
		return 1000L;
	}

	public long getConsumerLeaseTimeMillis() {
		return 20 * 1000L;
	}

	public long getConsumerLeaseClientSideAdjustmentTimeMills() {
		return -2 * 1000L;
	}

	public long getActiveConsumerCheckIntervalTimeMillis() {
		return 1000L;
	}

	public long getConsumerHeartbeatTimeoutMillis() {
		return getConsumerLeaseTimeMillis() + 3000L;
	}

	public long getBrokerHeartbeatTimeoutMillis() {
		return getBrokerLeaseTimeMillis() + 5000L;
	}

	public long getActiveBrokerCheckIntervalTimeMillis() {
		return 1000L;
	}

	public long getBrokerLeaseTimeMillis() {
		return 30 * 1000L;
	}

	public long getBrokerLeaseClientSideAdjustmentTimeMills() {
		return -3 * 1000L;
	}

	public String getMetaServerName() {
		return JSON.toJSONString(new HostPort(Networks.forIp().getLocalHostAddress(), getMetaServerPort()));
	}

	public int getMetaServerPort() {
		String port = System.getProperty("metaServerPort");
		if (StringUtils.isBlank(port)) {
			port = m_env.getGlobalConfig().getProperty("meta.port", "80");
		}

		if (StringUtils.isNumeric(port)) {
			return Integer.valueOf(port);
		} else {
			return 80;
		}

	}

	public String getMetaServerLeaderElectionZkPath() {
		return "/meta-servers";
	}
}
