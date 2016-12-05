package com.ctrip.hermes.ctrip.env;

import java.util.Properties;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface EnvProvider {
	public String getEnv();

	public void initialize(Properties config);

	public String getMetaServerDomainName();

	public String getIdc();
}
