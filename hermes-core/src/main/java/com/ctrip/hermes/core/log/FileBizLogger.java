package com.ctrip.hermes.core.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Named;
import org.unidal.net.Networks;

import com.alibaba.fastjson.JSON;

@Named(value = "FileBizLogger", type = BizLogger.class)
public class FileBizLogger implements BizLogger {

	private final static Logger log = LoggerFactory.getLogger(LoggerNames.BIZ);

	private final static String m_localhost = Networks.forIp().getLocalHostAddress();

	@Override
	public void log(BizEvent event) {
		event.addData("brokerIp", m_localhost);
		log.info(JSON.toJSONString(event));
	}
}
