package com.ctrip.hermes.portal.service.mail;

import com.ctrip.hermes.portal.dal.application.Application;

public interface PortalMailService {
	public void sendApplicationMail(Application app);
}
