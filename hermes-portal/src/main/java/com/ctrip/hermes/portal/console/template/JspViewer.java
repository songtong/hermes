package com.ctrip.hermes.portal.console.template;

import com.ctrip.hermes.portal.console.ConsolePage;
import org.unidal.web.mvc.view.BaseJspViewer;

public class JspViewer extends BaseJspViewer<ConsolePage, Action, Context, Model> {
	@Override
	protected String getJspFilePath(Context ctx, Model model) {
		Action action = model.getAction();

		switch (action) {
		case VIEW:
			return JspFile.VIEW.getPath() + ctx.getRequestContext().getUrlMapping().getPathInfo();
		}

		throw new RuntimeException("Unknown action: " + action);
	}
}
