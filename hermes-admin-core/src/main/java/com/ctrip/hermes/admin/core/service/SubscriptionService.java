package com.ctrip.hermes.admin.core.service;

import java.util.ArrayList;
import java.util.List;

import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.admin.core.model.Subscription;
import com.ctrip.hermes.admin.core.model.SubscriptionDao;
import com.ctrip.hermes.admin.core.model.SubscriptionEntity;
import com.ctrip.hermes.admin.core.view.SubscriptionView;

@Named
public class SubscriptionService {
	public static final String QMQ_TYPE = "qmq";

	public enum SubscriptionStatus {
		RUNNING, STOPPED
	}

	@Inject
	private SubscriptionDao m_subscriptionDao;

	public List<SubscriptionView> getSubscriptions(SubscriptionStatus status) throws DalException {
		List<Subscription> daoList = m_subscriptionDao.findByStatus(status.name(), SubscriptionEntity.READSET_FULL);
		List<SubscriptionView> result = new ArrayList<>();
		for (Subscription sub : daoList) {
			SubscriptionView view = toSubscriptionView(sub);
			result.add(view);
		}
		return result;
	}

	public List<SubscriptionView> getSubscriptions() throws DalException {
		List<Subscription> daoList = m_subscriptionDao.list(SubscriptionEntity.READSET_FULL);
		List<SubscriptionView> result = new ArrayList<>();
		for (Subscription sub : daoList) {
			SubscriptionView view = toSubscriptionView(sub);
			result.add(view);
		}
		return result;
	}

	public SubscriptionView findSubscriptionByTypeAndTopic(String type, String topic) throws DalException {
		return toSubscriptionView(m_subscriptionDao.findByTypeAndTopic(type, topic, SubscriptionEntity.READSET_FULL));
	}

	public SubscriptionView create(SubscriptionView view) throws DalException {
		Subscription sub = toSubscriptionModel(view);
		sub.setStatus(SubscriptionStatus.STOPPED.name());
		m_subscriptionDao.insert(sub);
		return toSubscriptionView(sub);
	}

	public SubscriptionView update(SubscriptionView view) throws DalException {
		Subscription sub = toSubscriptionModel(view);
		m_subscriptionDao.updateByPK(sub, SubscriptionEntity.UPDATESET_FULL);
		return toSubscriptionView(sub);
	}

	public void remove(long id) throws DalException {
		Subscription subscription = m_subscriptionDao.findByPK(id, SubscriptionEntity.READSET_FULL);
		if (subscription != null) {
			m_subscriptionDao.deleteByPK(subscription);
		}
	}

	public void start(long id) throws DalException {
		Subscription subscription = m_subscriptionDao.findByPK(id, SubscriptionEntity.READSET_FULL);
		if (subscription != null) {
			subscription.setStatus(SubscriptionStatus.RUNNING.name());
			m_subscriptionDao.updateByPK(subscription, SubscriptionEntity.UPDATESET_FULL);
		}
	}

	public void stop(long id) throws DalException {
		Subscription subscription = m_subscriptionDao.findByPK(id, SubscriptionEntity.READSET_FULL);
		if (subscription != null) {
			subscription.setStatus(SubscriptionStatus.STOPPED.name());
			m_subscriptionDao.updateByPK(subscription, SubscriptionEntity.UPDATESET_FULL);
		}
	}

	public static Subscription toSubscriptionModel(SubscriptionView view) {
		Subscription sub = new Subscription();
		sub.setId(view.getId());
		sub.setGroup(view.getGroup());
		sub.setTopic(view.getTopic());
		sub.setEndpoints(view.getEndpoints());
		sub.setName(view.getName());
		sub.setStatus(view.getStatus());
		sub.setType(view.getType());

		return sub;
	}

	public static SubscriptionView toSubscriptionView(Subscription sub) {
		SubscriptionView view = new SubscriptionView();
		view.setId(sub.getId());
		view.setGroup(sub.getGroup());
		view.setTopic(sub.getTopic());
		view.setEndpoints(sub.getEndpoints());
		view.setName(sub.getName());
		view.setStatus(sub.getStatus());
		view.setType(sub.getType());

		return view;
	}
}
