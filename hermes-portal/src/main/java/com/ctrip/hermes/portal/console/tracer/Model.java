package com.ctrip.hermes.portal.console.tracer;

import com.ctrip.hermes.portal.console.ConsolePage;
import org.unidal.web.mvc.ViewModel;

public class Model extends ViewModel<ConsolePage, Action, Context> {

	private String m_esHost;

	public Model(Context ctx) {
		super(ctx);
	}

	@Override
	public Action getDefaultAction() {
		return Action.VIEW;
	}

	public String getEsHost() {
		return m_esHost;
	}

	public void setEsHost(String esHost) {
		m_esHost = esHost;
	}
}
