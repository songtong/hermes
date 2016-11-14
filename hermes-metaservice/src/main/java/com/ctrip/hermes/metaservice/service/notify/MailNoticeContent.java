package com.ctrip.hermes.metaservice.service.notify;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.env.ClientEnvironment;

public abstract class MailNoticeContent implements HermesNoticeContent {

	protected static final String m_env = PlexusComponentLocator.lookup(ClientEnvironment.class).getEnv();

	@Override
	public HermesNoticeType getType() {
		return HermesNoticeType.EMAIL;
	}

}
