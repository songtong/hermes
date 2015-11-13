package com.ctrip.hermes.portal.service.application;

import java.util.List;

import com.ctrip.hermes.core.bo.TopicView;
import com.ctrip.hermes.portal.application.HermesApplication;
import com.ctrip.hermes.portal.application.TopicApplication;

public interface ApplicationService {

	public TopicApplication saveTopicApplication(TopicApplication topicApplication);

	public HermesApplication getApplicationById(long id);

	public List<HermesApplication> getApplicationsByStatus(int Status);

	public TopicView generageTopicView(TopicApplication app);

	public HermesApplication updateApplication(HermesApplication app);

	public HermesApplication updateStatus(long id, int status, String comment, String approver);

}
