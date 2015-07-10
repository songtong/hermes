package com.ctrip.hermes.portal.console.topic;

import java.io.IOException;
import java.util.List;

import javax.servlet.ServletException;

import org.unidal.lookup.annotation.Inject;
import org.unidal.web.mvc.PageHandler;
import org.unidal.web.mvc.annotation.InboundActionMeta;
import org.unidal.web.mvc.annotation.OutboundActionMeta;
import org.unidal.web.mvc.annotation.PayloadMeta;

import com.ctrip.hermes.core.utils.CollectionUtil;
import com.ctrip.hermes.core.utils.CollectionUtil.Transformer;
import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.metaservice.service.TopicService;
import com.ctrip.hermes.portal.config.PortalConfig;
import com.ctrip.hermes.portal.console.ConsolePage;

public class Handler implements PageHandler<Context> {
	@Inject
	private JspViewer m_jspViewer;

	@Inject
	private TopicService m_topicService;

	@Inject
	private PortalConfig m_config;

	@Override
	@PayloadMeta(Payload.class)
	@InboundActionMeta(name = "topic")
	public void handleInbound(Context ctx) throws ServletException, IOException {
		// display only, no action here
	}

	@Override
	@SuppressWarnings("unchecked")
	@OutboundActionMeta(name = "topic")
	public void handleOutbound(Context ctx) throws ServletException, IOException {
		Model model = new Model(ctx);
		model.setAction(ctx.getPayload().getAction());
		model.setPage(ConsolePage.TOPIC);

		String topicName = ctx.getPayload().getTopic();
		switch (model.getAction()) {
		case DETAIL:
			model.setTopicName(topicName);
			List<String> list = (List<String>) CollectionUtil.collect(m_topicService.findTopicByName(topicName)
			      .getConsumerGroups(), new Transformer() {
				@Override
				public String transform(Object obj) {
					return ((ConsumerGroup) obj).getName();
				}
			});
			System.out.println(list);
			model.setConsumers(list);
			model.setKibanaUrl(m_config.getKibanaBaseUrl());
			break;
		default:
			break;
		}

		if (!ctx.isProcessStopped()) {
			m_jspViewer.view(ctx, model);
		}
	}
}
