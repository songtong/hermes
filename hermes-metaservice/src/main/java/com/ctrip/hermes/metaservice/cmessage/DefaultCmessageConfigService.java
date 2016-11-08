package com.ctrip.hermes.metaservice.cmessage;

import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.cmessaging.entity.Cmessaging;
import com.ctrip.hermes.cmessaging.entity.Exchange;
import com.ctrip.hermes.metaservice.service.KVService;
import com.ctrip.hermes.metaservice.service.KVService.Tag;

@Named(type = CmessageConfigService.class)
public class DefaultCmessageConfigService implements CmessageConfigService {
	private static final String CMSG_VERSION_KEY = "_#cmsg_version#_";

	private static final Logger log = LoggerFactory.getLogger(DefaultCmessageConfigService.class);

	@Inject
	private KVService m_kvService;

	@Override
	public Cmessaging getCmessaging() {
		Map<String, String> map = m_kvService.list(Tag.CMSG);
		Cmessaging cmsg = new Cmessaging();
		for (Entry<String, String> entry : map.entrySet()) {
			try {
				if (!CMSG_VERSION_KEY.equals(entry.getKey())) {
					Exchange exchange = JSON.parseObject(entry.getValue(), Exchange.class);
					cmsg.addExchange(exchange);
				} else {
					cmsg.setVersion(Long.valueOf(entry.getValue()));
				}
			} catch (Exception e) {
				log.error("Parse exchange failed, json: {}", entry.getValue());
				throw new RuntimeException("Load cmessaging from db failed.", e);
			}
		}
		return cmsg;
	}

	@Override
	public void updateCmessaging(String cmessagingStr) {
		Cmessaging cmsg = JSON.parseObject(cmessagingStr, Cmessaging.class);
		for (Entry<String, Exchange> entry : cmsg.getExchanges().entrySet()) {
			try {
				m_kvService.setKeyValue(entry.getKey(), JSON.toJSONString(entry.getValue()), Tag.CMSG);
			} catch (Exception e) {
				log.error("Update cmessage exchange config failed: {}, {}", entry.getKey(), entry.getValue());
			}
		}
		try {
			m_kvService.setKeyValue(CMSG_VERSION_KEY, String.valueOf(cmsg.getVersion()), Tag.CMSG);
		} catch (Exception e) {
			log.error("Update cmessage config version failed, version: {}", cmsg.getVersion());
		}
	}
}
