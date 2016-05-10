package com.ctrip.hermes.collector.rule;

import java.util.Observable;
import java.util.Observer;

import com.ctrip.hermes.collector.state.State;


public abstract class Rule extends Observable {
	private RuleContext m_context;
	
	public Rule(RuleContext context) {
		this.m_context = context;
	}
	
	public abstract boolean validate(State state);
		
	public void apply(State state) {
		if (validate(state)) {
			notifyObservers(m_context);
		}
	}
	
	public abstract class RuleEvent implements Observer {
		// Test whether should trigger this event handler.
		public abstract boolean match(RuleContext context);
		// Get event handler.
		public abstract RuleEventHandler eventHandler();
		
	    public void update(Observable rule, Object context) {
	    	RuleContext ctx = (RuleContext)context;
	    	if (match(ctx)) {
	    		eventHandler().handleEvent(ctx);
	    	}
	    }
	}
	
	public interface RuleEventHandler {
		// Handle the event.
		public void handleEvent(RuleContext context);
	}
	
	public class RuleContext {
		private State state = null;

		public State getState() {
			return state;
		}

		public void setState(State state) {
			this.state = state;
		}
	}
}
