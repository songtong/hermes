package com.ctrip.hermes.portal.console.tracer;

import java.io.IOException;
import java.util.Collections;

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
	@InboundActionMeta(name = "tracer")
	public void handleInbound(Context ctx) throws ServletException, IOException {
		// display only, no action here
	}

	@Override
	@OutboundActionMeta(name = "tracer")
	public void handleOutbound(Context ctx) throws ServletException, IOException {
		Model model = new Model(ctx);
		Collections.shuffle(m_config.getElasticClusterNodes());
		model.setEsHost(m_config.getElasticClusterNodes().get(0).getKey());

		model.setAction(Action.VIEW);
		model.setPage(ConsolePage.TRACER);

		if (!ctx.isProcessStopped()) {
			m_jspViewer.view(ctx, model);
		}
	}
}
