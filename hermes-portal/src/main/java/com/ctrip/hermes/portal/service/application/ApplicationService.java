package com.ctrip.hermes.portal.service.application;

import java.util.List;

import com.ctrip.hermes.metaservice.view.ConsumerGroupView;
import com.ctrip.hermes.metaservice.view.TopicView;
import com.ctrip.hermes.portal.application.ConsumerApplication;
import com.ctrip.hermes.portal.application.HermesApplication;
import com.ctrip.hermes.portal.application.TopicApplication;

public interface ApplicationService {

	public TopicApplication saveTopicApplication(TopicApplication topicApplication);

	public ConsumerApplication saveConsumerApplication(ConsumerApplication consumerApplication);

	public HermesApplication getApplicationById(long id);

	public List<HermesApplication> getApplicationsByStatus(int Status);

	public TopicView generateTopicView(TopicApplication app);

	public ConsumerGroupView generateConsumerView(ConsumerApplication app);

	public HermesApplication updateApplication(HermesApplication app);

	public HermesApplication updateStatus(long id, int status, String comment, String approver);

}
