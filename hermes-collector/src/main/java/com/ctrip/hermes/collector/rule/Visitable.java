package com.ctrip.hermes.collector.rule;

public interface Visitable {
	public void accept(Visitor visitor);
}
