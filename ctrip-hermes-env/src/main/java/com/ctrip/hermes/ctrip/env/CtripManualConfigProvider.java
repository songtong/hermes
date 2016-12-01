package com.ctrip.hermes.ctrip.env;

import org.unidal.lookup.annotation.Named;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigFile;
import com.ctrip.framework.apollo.ConfigService;
import com.ctrip.framework.apollo.core.enums.ConfigFileFormat;
import com.ctrip.hermes.env.ManualConfigProvider;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ManualConfigProvider.class)
public class CtripManualConfigProvider implements ManualConfigProvider {

	@Override
	public boolean isManualConfigureModeOn() {
		Config config = ConfigService.getConfig("FX.hermes.client");
		return config.getBooleanProperty("manual.config.enabled", false);
	}

	@Override
	public String fetchManualConfig() {
		ConfigFile file = ConfigService.getConfigFile("config.manual", ConfigFileFormat.XML);
		return file.getContent();
	}

	@Override
	public String getBrokers() {
		Config config = ConfigService.getConfig("FX.hermes.client");
		return config.getProperty("brokers", "[]");
	}

}
