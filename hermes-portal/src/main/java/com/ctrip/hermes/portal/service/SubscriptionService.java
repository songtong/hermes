package com.ctrip.hermes.portal.service;

import java.util.Map;

import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Subscription;

@Named
public class SubscriptionService {
	
	@Inject
	private MetaServiceWrapper m_metaService;

	public Map<String, Subscription> getSubscriptions() {
		return m_metaService.getSubscriptions();
	}

	public void create(Subscription subscription) throws DalException {
		Meta meta = m_metaService.getMeta();
		meta.addSubscription(subscription);
		if (!m_metaService.updateMeta(meta)) {
			throw new RuntimeException("Update meta failed, please try later");
		}
	}

	public void remove(String id) throws DalException {
		Meta meta = m_metaService.getMeta();
		meta.removeSubscription(id);
		if (!m_metaService.updateMeta(meta)) {
			throw new RuntimeException("Update meta failed, please try later");
		}
	}
}
