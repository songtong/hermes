package com.ctrip.hermes.portal.service.mail;

import com.ctrip.hermes.metaservice.view.SchemaView;
import com.ctrip.hermes.portal.dal.application.Application;

public interface PortalMailService {
	public void sendApplicationMail(Application app);

	public void sendUploadSchemaMail(SchemaView schema, String mailAddress, String userName);
}
