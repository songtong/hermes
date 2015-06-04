package com.ctrip.hermes.core.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;

@Named(type = BizLogger.class)
public class DefaultBizLogger implements BizLogger {

	private final static Logger log = LoggerFactory.getLogger(LoggerNames.BIZ);

	@Override
	public void log(BizEvent event) {
		log.info(JSON.toJSONString(event));
	}

}
