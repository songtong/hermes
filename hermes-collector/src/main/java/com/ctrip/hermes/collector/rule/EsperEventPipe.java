package com.ctrip.hermes.collector.rule;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.collector.pipe.Pipe;
import com.ctrip.hermes.collector.pipe.PipeContext;
import com.ctrip.hermes.collector.rule.annotation.Context;
import com.ctrip.hermes.collector.rule.annotation.EPL;
import com.ctrip.hermes.collector.rule.annotation.Event;
import com.ctrip.hermes.collector.rule.annotation.Pattern;
import com.ctrip.hermes.collector.rule.annotation.Table;
import com.ctrip.hermes.collector.rule.annotation.Window;
import com.ctrip.hermes.collector.state.State;
import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.espertech.esper.collection.Pair;

public class EsperEventPipe<T> extends Pipe<T> implements Visitor {
	private static final Logger LOGGER = LoggerFactory.getLogger(EsperEventPipe.class); 
	private RuleEventHandler m_eventHandler; 
	private EPRuntime m_runtime;
	private EPAdministrator m_administrator;
	
	public EsperEventPipe(Class<?> handledType, RuleEventHandler eventHandler) {
		super(handledType);
		this.m_eventHandler = eventHandler;
	}
	
	public void visit(Visitable visitable) {
		RuleEventHandler handler = (RuleEventHandler)visitable;
		visitTypes(handler.findHandledTypes());
		visitUtils(handler.findUtils());
		visitTables(handler.findTables());
		visitWindows(handler.findWindows());
		visitContexts(handler.findContexts());
		visitEPLs(handler.findEPLs());
		visitPatterns(handler.findPatterns());
	}
	
	private void registerStatement(String statementName, EPStatement statement) {
		if (statement != null) {
			m_eventHandler.registerStatement(statementName, statement);
			statement.addListener(new RuleEventListener(statementName));
			LOGGER.info("Created statement [{}] listener for handler: {}", statementName, m_eventHandler.getClass().getName());
		}
	}
	
	public void setup(Configuration conf) {
		EPServiceProvider provider = EPServiceProviderManager.getProvider(m_eventHandler.getClass().getSimpleName(), conf);
		m_runtime = provider.getEPRuntime();
		m_administrator = provider.getEPAdministrator();
		m_eventHandler.accept(this);
		m_eventHandler.setEPAdministrator(this.m_administrator);
	}

	@Override
	public boolean validate(T obj) {
		return m_eventHandler.getHandledTypes().contains(obj.getClass());
	}

	@Override
	public synchronized void doProcess(PipeContext context, T obj) throws Exception {
		// Send event & processing in sync.
		m_runtime.sendEvent(obj);
	}
	
	@Override
	public void visitContexts(List<Context> contexts) {
		if (contexts != null && contexts.size() > 0) {
			for (Context context : contexts) {
				m_administrator.createEPL(context.value());
			}
		}
	}

	@Override
	public void visitTables(List<Table> tables) {
		if (tables != null && tables.size() > 0) {
			for (Table table : tables) {
				registerStatement(table.name(), m_administrator.createEPL(table.value()));
			}
		}
	}

	@Override
	public void visitWindows(List<Window> windows) {
		if (windows != null && windows.size() > 0) {
			for (Window window : windows) {
				registerStatement(window.name(), m_administrator.createEPL(window.value()));
			}
		}
	}

	@Override
	public void visitEPLs(List<EPL> epls) {
		if (epls != null && epls.size() > 0) {
			for (EPL epl : epls) {
				registerStatement(epl.name(), m_administrator.createEPL(epl.value()));
			}
		}
	}

	@Override
	public void visitPatterns(List<Pattern> patterns) {
		if (patterns != null && patterns.size() > 0) {
			for (Pattern pattern : patterns) {
				registerStatement(pattern.name(), m_administrator.createEPL(pattern.value()));
			}
		}
	}

	@Override
	public void visitTypes(List<Class<?>> handledTypes) {
		m_administrator.getConfiguration().addEventType(State.class.getSimpleName(), State.class);
		
		for (Class<?> handledType : handledTypes) {
			String eventName = handledType.getSimpleName();
			if (handledType.isAnnotationPresent(Event.class)) {
				Event e = handledType.getAnnotation(Event.class);
				eventName = e.name();
			}
			m_administrator.getConfiguration().addEventType(eventName, handledType);
			LOGGER.info("Register event {} with class {}.", eventName, handledType.getName());
		}
	}
	
	public void visitUtils(List<Class<?>> utils) {
		if (utils != null && utils.size() > 0) {
			for (Class<?> util : utils) {
				Method[] methods = util.getMethods();
				for (Method method : methods) {
					if (Modifier.isStatic(method.getModifiers())) {
						m_administrator.getConfiguration().addPlugInSingleRowFunction(method.getName(), util.getName(), method.getName());
					}
				}
			}
		}
	}

	public class RuleEventListener implements UpdateListener {
		private String m_statementName;
		
		public RuleEventListener(String statementName) {
			this.m_statementName = statementName;
		}

		@Override
		public void update(EventBean[] newEvents, EventBean[] oldEvents) {
			RuleEvent event = new RuleEvent();
			event.setEventSource(m_statementName);
			event.setData(Pair.createPair(newEvents, oldEvents));
			m_eventHandler.handleEvent(event);
		}
	}

}
