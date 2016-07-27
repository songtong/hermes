package com.ctrip.hermes.monitor.zabbix;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.zabbix4j.ZabbixApi;
import com.zabbix4j.ZabbixApiException;
import com.zabbix4j.host.HostObject;

@Service
@Scope("singleton")
public class ZabbixApiGateway1 extends ZabbixApiGateway {

	@PostConstruct
	private void postConstruct() {
		if (config.isMonitorCheckerEnable()) {
			zabbixApi = new ZabbixApi(config.getZabbixUrl1());
			try {
				zabbixApi.login(config.getZabbixUsername(), config.getZabbixPassword());
			} catch (ZabbixApiException e) {
				e.printStackTrace();
			}

			hostNameCache = CacheBuilder.newBuilder().recordStats().maximumSize(50).expireAfterWrite(1, TimeUnit.HOURS)
			      .build(new CacheLoader<String, Map<Integer, HostObject>>() {

				      @Override
				      public Map<Integer, HostObject> load(String name) throws Exception {
					      return populateHostsByName(name);
				      }

			      });
		}
	}
}
