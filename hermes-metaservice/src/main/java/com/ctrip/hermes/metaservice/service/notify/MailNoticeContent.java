package com.ctrip.hermes.metaservice.service.notify;

import com.ctrip.hermes.Hermes.Env;
import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;

public abstract class MailNoticeContent implements NoticeContent {

	protected static final Env m_env = PlexusComponentLocator.lookup(ClientEnvironment.class).getEnv();

	@Override
	public NoticeType getType() {
		return NoticeType.EMAIL;
	}

}
