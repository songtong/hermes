package com.ctrip.hermes.portal.application;

import com.ctrip.hermes.portal.dal.application.Application;

public class HermesApplicationParser {

	public static HermesApplication parse(Application dbApp) {
		HermesApplicationType type = HermesApplicationType.findByTypeCode(dbApp.getType());
		if (type != null) {
			try {
				HermesApplication app = type.getClazz().newInstance();
				// app.parse(dbApp);
				return app;
			} catch (Exception e) {

			}

		}
		return null;
	}

}
