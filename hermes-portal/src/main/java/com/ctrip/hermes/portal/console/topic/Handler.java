package com.ctrip.hermes.portal.console.topic;

import java.io.IOException;

import javax.servlet.ServletException;

import org.unidal.lookup.annotation.Inject;
import org.unidal.web.mvc.PageHandler;
import org.unidal.web.mvc.annotation.InboundActionMeta;
import org.unidal.web.mvc.annotation.OutboundActionMeta;
import org.unidal.web.mvc.annotation.PayloadMeta;

import com.ctrip.hermes.metaservice.service.TopicService;
import com.ctrip.hermes.portal.console.ConsolePage;

public class Handler implements PageHandler<Context> {
	@Inject
	private JspViewer m_jspViewer;

	@Inject
	private TopicService m_topicService;

	@Override
	@PayloadMeta(Payload.class)
	@InboundActionMeta(name = "topic")
	public void handleInbound(Context ctx) throws ServletException, IOException {
		// display only, no action here
	}

	@Override
	@OutboundActionMeta(name = "topic")
	public void handleOutbound(Context ctx) throws ServletException, IOException {
		Model model = new Model(ctx);
		model.setAction(ctx.getPayload().getAction());
		model.setPage(ConsolePage.TOPIC);

		switch (model.getAction()) {
		case DETAIL:
			model.setTopicName(ctx.getHttpServletRequest().getParameter("topic"));
			break;
		default:
			break;
		}

		if (!ctx.isProcessStopped()) {
			m_jspViewer.view(ctx, model);
		}
	}
}
