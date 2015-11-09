package com.ctrip.hermes.portal.config;

import org.unidal.net.Networks;

public class PortalConstants {
	public static final String LOCALHOST = Networks.forIp().getLocalHostAddress();

	public static final int PRIORITY_TRUE = 0;

	public static final int PRIORITY_FALSE = 1;

	public static final int APP_TYPE_CREATE_TOPIC = 0;

	public static final int APP_TYPE_CREATE_CONSUMER = 1;

	public static final int APP_TYPE_MODIFY_TOPIC = 2;

	public static final int APP_TYPE_MODIFY_CONSUMER = 3;

	public static final int APP_STATUS_PROCESSING = 0;
	
	public static final int APP_STATUS_REJECTED = 1;
	
	public static final int APP_STATUS_SUCCESS = 2;	
	

}
