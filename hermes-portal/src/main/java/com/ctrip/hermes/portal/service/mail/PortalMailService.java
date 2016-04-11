package com.ctrip.hermes.portal.service.mail;

import com.ctrip.hermes.metaservice.view.SchemaView;
import com.ctrip.hermes.portal.application.HermesApplication;

public interface PortalMailService {
	public void sendApplicationMail(HermesApplication app);

	public void sendUploadSchemaMail(SchemaView schema, String mailAddress, String userName);
}
