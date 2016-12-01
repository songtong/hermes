package com.ctrip.hermes.collector.record;

public interface Bindable<T> {
	public void bind(T json);
}
