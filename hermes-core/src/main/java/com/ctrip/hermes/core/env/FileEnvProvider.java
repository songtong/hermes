package com.ctrip.hermes.core.env;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.Hermes.Env;
import com.ctrip.hermes.core.utils.StringUtils;

@Named(type = EnvProvider.class)
public class FileEnvProvider implements EnvProvider, Initializable {

	@Inject
	private ClientEnvironment m_clientEnv;

	private Env m_env;

	@Override
	public Env getEnv() {
		return m_env;
	}

	@Override
	public void initialize() throws InitializationException {
		String strEnv = m_clientEnv.getGlobalConfig().getProperty("env");
		if (!StringUtils.isBlank(strEnv)) {
			try {
				m_env = Env.valueOf(strEnv.trim().toUpperCase());
			} catch (RuntimeException e) {
				throw new InitializationException(
				      String.format("%s is not a valid hermes env, valid values are (dev, fws, uat, lpt, prod)", strEnv));
			}
		}
	}

}
