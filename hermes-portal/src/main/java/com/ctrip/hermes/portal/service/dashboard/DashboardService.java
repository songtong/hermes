package com.ctrip.hermes.portal.service.dashboard;

import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.StreamingOutput;

import org.unidal.dal.jdbc.DalException;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.portal.resource.view.TopicDelayDetailView.DelayDetail;
import com.ctrip.hermes.portal.service.dashboard.DefaultDashboardService.DeadletterView;

public interface DashboardService {

	public Date getLatestProduced(String topic);

	public List<DelayDetail> getDelayDetailForConsumer(String topic, String consumer);

	public List<Pair<String, Date>> getTopOutdateTopic(int top);

	public Map<String, Map<String, Map<String, Long>>> getTopicOffsetLags();

	public List<DeadletterView> getLatestDeadLetter(String topic, String consumer, int count) throws DalException;

	public StreamingOutput getDeadLetterStreamByTimespan(String topic, String consumer, Date timeStart, Date timeEnd,
	      long sleepIntelval);

}
