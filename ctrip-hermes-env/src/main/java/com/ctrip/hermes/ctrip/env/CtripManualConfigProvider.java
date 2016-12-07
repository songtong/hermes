package com.ctrip.hermes.ctrip.env;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
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
public class CtripManualConfigProvider implements ManualConfigProvider, Initializable {

	@Override
	public boolean isManualConfigureModeOn() {
		Config config = ConfigService.getConfig("FX.hermes.client");
		return config.getBooleanProperty("manual.config.enabled", false);
	}

	@Override
	public String fetchManualConfig() {
		ConfigFile file = ConfigService.getConfigFile("config.manual", ConfigFileFormat.JSON);
		return file.getContent();
	}

	@Override
	public String getBrokers() {
		Config config = ConfigService.getConfig("FX.hermes.client");
		return config.getProperty("brokers", "[]");
	}

	@Override
	public void initialize() throws InitializationException {
		// warmup call
		fetchManualConfig();
	}

}
