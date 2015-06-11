package com.ctrip.hermes.metaservice.service;

import java.util.ArrayList;
import java.util.List;

import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.bo.SubscriptionView;
import com.ctrip.hermes.metaservice.model.Subscription;
import com.ctrip.hermes.metaservice.model.SubscriptionDao;
import com.ctrip.hermes.metaservice.model.SubscriptionEntity;

@Named
public class SubscriptionService {

	@Inject
	private SubscriptionDao m_subscriptionDao;

	public List<SubscriptionView> getSubscriptions() throws DalException {
		List<Subscription> daoList = m_subscriptionDao.list(SubscriptionEntity.READSET_FULL);
		List<SubscriptionView> result = new ArrayList<>();
		for (Subscription sub : daoList) {
			SubscriptionView view = toSubscriptionView(sub);
			result.add(view);
		}
		return result;
	}

	public SubscriptionView create(SubscriptionView view) throws DalException {
		Subscription sub = toSubscription(view);
		m_subscriptionDao.insert(sub);
		return toSubscriptionView(sub);
	}

	public void remove(long id) throws DalException {
		Subscription subscription = m_subscriptionDao.findByPK(id, SubscriptionEntity.READSET_FULL);
		if (subscription != null) {
			m_subscriptionDao.deleteByPK(subscription);
		}
	}

	public static Subscription toSubscription(SubscriptionView view) {
		Subscription sub = new Subscription();
		sub.setId(view.getId());
		sub.setGroup(view.getGroup());
		sub.setTopic(view.getTopic());
		sub.setEndpoints(view.getEndpoints());
		sub.setName(view.getName());
		return sub;
	}

	public static SubscriptionView toSubscriptionView(Subscription sub) {
		SubscriptionView view = new SubscriptionView();
		view.setId(sub.getId());
		view.setGroup(sub.getGroup());
		view.setTopic(sub.getTopic());
		view.setEndpoints(sub.getEndpoints());
		view.setName(sub.getName());
		return view;
	}
}
