package com.ctrip.hermes.portal.config;

import org.unidal.net.Networks;

public class PortalConstants {
	public static final String LOCALHOST = Networks.forIp().getLocalHostAddress();

	public static final int PRIORITY_TRUE = 0;

	public static final int PRIORITY_FALSE = 1;
}
