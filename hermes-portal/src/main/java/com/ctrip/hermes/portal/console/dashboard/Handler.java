package com.ctrip.hermes.portal.console.dashboard;

import java.io.IOException;

import javax.servlet.ServletException;

import org.unidal.lookup.annotation.Inject;
import org.unidal.web.mvc.PageHandler;
import org.unidal.web.mvc.annotation.InboundActionMeta;
import org.unidal.web.mvc.annotation.OutboundActionMeta;
import org.unidal.web.mvc.annotation.PayloadMeta;

import com.ctrip.hermes.portal.config.PortalConfig;
import com.ctrip.hermes.portal.console.ConsolePage;

public class Handler implements PageHandler<Context> {
	@Inject
	private JspViewer m_jspViewer;

	@Inject
	private PortalConfig m_config;

	@Override
	@PayloadMeta(Payload.class)
	@InboundActionMeta(name = "dashboard")
	public void handleInbound(Context ctx) throws ServletException, IOException {
		// display only, no action here
	}

	@Override
	@OutboundActionMeta(name = "dashboard")
	public void handleOutbound(Context ctx) throws ServletException, IOException {
		Payload payload = ctx.getPayload();
		Action action = payload.getAction();
		Model model = new Model(ctx);
		model.setAction(action);
		model.setPage(ConsolePage.DASHBOARD);
		model.setKibanaUrl(m_config.getKibanaBaseUrl());

		switch (action) {
		case TOPIC_DETAIL:
			model.setTopic(payload.getTopic());
			break;
		case BROKER_DETAIL:
			model.setBrokerIP(payload.getBroker());
			break;
		case CLIENT:
			model.setClientIP(payload.getClient());
			break;
		default:
			break;
		}

		if (!ctx.isProcessStopped()) {
			m_jspViewer.view(ctx, model);
		}
	}
}
