package com.ctrip.hermes.collector.rule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

/**
 * @author tenglinxiao
 *
 */
@Component
public class RuleEventHandlerRegistry implements ApplicationContextAware {
	private static final Logger LOGGER = LoggerFactory.getLogger(RuleEventHandlerRegistry.class);
	private Map<Class<?>, List<RuleEventHandler>> m_eventHandlers = new HashMap<>();
	private Map<RuleEventHandler, List<Class<?>>> m_handlerEvents = new HashMap<>();
	private ApplicationContext m_applicationContext;
	
	@PostConstruct
	protected void init() {
		Map<String, RuleEventHandler> handlers = this.m_applicationContext.getBeansOfType(RuleEventHandler.class);
		for (RuleEventHandler handler : handlers.values()) {
			List<Class<?>> handledTypes = handler.getHandledTypes();
			if (handledTypes == null || handledTypes.size() == 0) {
				LOGGER.warn("Ignore event handler due to no triggered type defined.");
				continue;
			}
			
			for (Class<?> handledType : handledTypes) {
				if (!m_eventHandlers.containsKey(handledType)) {
					m_eventHandlers.put(handledType, new ArrayList<RuleEventHandler>());
				}
				m_eventHandlers.get(handledType).add(handler);
			}
			
			m_handlerEvents.put(handler, handledTypes);
		}
	}
	
	public void setApplicationContext(ApplicationContext applicationContext) {
		this.m_applicationContext = applicationContext;
	}
	
	public ApplicationContext getApplicationContext() {
		return this.m_applicationContext;
	}
	
	// Get all registered types.
	public Set<Class<?>> getRegisteredTypes() {
		return m_eventHandlers.keySet();
	}
	
	public Set<RuleEventHandler> getRegisteredHandlers() {
		return m_handlerEvents.keySet();
	}
	
	public List<RuleEventHandler> getRuleEventHandlers(Class<?> handledType) {
		return m_eventHandlers.get(handledType);
	}

}
