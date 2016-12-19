package com.ctrip.hermes.portal.service.application;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.admin.core.service.ConsumerService;
import com.ctrip.hermes.admin.core.service.TopicService;
import com.ctrip.hermes.admin.core.view.ConsumerGroupView;
import com.ctrip.hermes.admin.core.view.TopicView;
import com.ctrip.hermes.core.constants.IdcPolicy;
import com.ctrip.hermes.portal.application.ConsumerApplication;
import com.ctrip.hermes.portal.application.HermesApplication;
import com.ctrip.hermes.portal.application.TopicApplication;
import com.ctrip.hermes.portal.config.PortalConfig;
import com.ctrip.hermes.portal.dal.application.Application;
import com.ctrip.hermes.portal.dal.application.HermesApplicationDao;
import com.ctrip.hermes.portal.service.mail.PortalMailService;

@Named(type = ApplicationService.class)
public class DefaultApplicationService implements ApplicationService {
	private static final Logger log = LoggerFactory.getLogger(DefaultApplicationService.class);

	@Inject
	private HermesApplicationDao m_dao;

	@Inject
	private PortalMailService m_mailService;

	@Inject
	private TopicService m_topicService;

	@Inject
	private ConsumerService m_consumerService;

	@Inject
	private PortalConfig m_config;

	@Override
	public TopicApplication saveTopicApplication(TopicApplication topicApplication) {
		Application dbApp = HermesApplication.toDBEntity(topicApplication);
		try {
			m_dao.saveApplication(dbApp);
		} catch (DalException e) {
			log.error("Create new topic application : {}.{}.{} failed", topicApplication.getProductLine(),
			      topicApplication.getEntity(), topicApplication.getEvent(), e);
			return null;
		}

		return (TopicApplication) HermesApplication.parse(dbApp);
	}

	@Override
	public HermesApplication getApplicationById(long id) {
		Application dbApp = null;
		try {
			dbApp = m_dao.getAppById(id);
			return HermesApplication.parse(dbApp);
		} catch (DalException e) {
			log.error("Read application:id={} from db failed.", id, e);
		}
		return null;
	}

	@Override
	public List<HermesApplication> getApplicationsByOwnerStatus(String owner, int status, int offset, int size) {
		List<HermesApplication> applications = new ArrayList<HermesApplication>();
		List<Application> dbApps = null;
		try {
			dbApps = m_dao.getApplicationsByOwnerStatus(owner, status, offset, size);
			for (Application dbApp : dbApps) {
				applications.add(HermesApplication.parse(dbApp));
			}
			return applications;
		} catch (DalException e) {
			log.error("Read applications: status={} from db failed.", status, e);
		}
		return new ArrayList<>();
	}

	@Override
	public TopicView generateTopicView(TopicApplication app) {
		return PartitionStrategy.getStategy(app.getStorageType()).apply(app);
	}

	@Override
	public HermesApplication updateApplication(HermesApplication app) {
		Application dbApp = null;
		try {
			dbApp = HermesApplication.toDBEntity(app);
			dbApp = m_dao.updateApplication(dbApp);
			app = HermesApplication.parse(dbApp);
		} catch (Exception e) {
			log.error("Update application:id={} failed!", app.getId(), e);
			return null;
		}

		return app;

	}

	@Override
	public HermesApplication updateStatus(long id, int status, String comment, String approver) {
		Application dbApp = null;
		try {
			dbApp = m_dao.getAppById(id);

			dbApp.setStatus(status);
			dbApp.setComment(comment);
			dbApp.setApprover(approver);
			dbApp = m_dao.updateApplication(dbApp);
		} catch (DalException e) {
			log.error("Update status of apllication: id={} failed.", id, e);
			return null;
		}

		return HermesApplication.parse(dbApp);
	}

	@Override
	public ConsumerApplication saveConsumerApplication(ConsumerApplication consumerApplication) {
		Application dbApp = null;
		try {
			dbApp = HermesApplication.toDBEntity(consumerApplication);
			m_dao.saveApplication(dbApp);
		} catch (DalException e) {
			log.error("Create new consumer application : {}.{}.{} failed", consumerApplication.getProductLine(),
			      consumerApplication.getProduct(), consumerApplication.getProject(), e);
			return null;
		}

		return (ConsumerApplication) HermesApplication.parse(dbApp);
	}

	@Override
	public ConsumerGroupView generateConsumerView(ConsumerApplication app) {
		ConsumerGroupView consumerView = new ConsumerGroupView();
		consumerView.setOrderedConsume(true);
		consumerView.setTopicName(app.getTopicName());
		consumerView.setName(app.getProductLine() + "." + app.getProduct() + "." + app.getProject());
		consumerView.setAckTimeoutSeconds(app.getAckTimeoutSeconds());
		consumerView.setAppIds(app.getAppName());
		consumerView.setOwner1(app.getOwnerName1() + "/" + app.getOwnerEmail1());
		consumerView.setOwner2(app.getOwnerName2() + "/" + app.getOwnerEmail2());
		consumerView.setPhone1(app.getOwnerPhone1());
		consumerView.setPhone2(app.getOwnerPhone2());
		consumerView.setEnabled(app.isEnabled());
		if (app.isNeedMultiIdc()) {
			consumerView.setIdcPolicy(IdcPolicy.LOCAL);
		} else {
			consumerView.setIdcPolicy(IdcPolicy.PRIMARY);
		}
		if (app.isNeedRetry()) {
			consumerView.setRetryPolicy(String.format("3:[%s,%s]", app.getRetryCount(), app.getRetryInterval() * 1000L));
		} else {
			consumerView.setRetryPolicy("2:[]");
		}

		return consumerView;
	}

	@Override
	public int countApplicationsByOwnerStatus(String owner, int status) {
		try {
			if (owner == null) {
				return m_dao.countApplicationsByStatus(status);
			}
			return m_dao.countApplicationsByOwnerStatus(owner, status);
		} catch (DalException e) {
			log.error("Count application:owner={}, status={} from db failed.", owner, status, e);
		}
		return -1;
	}
}
