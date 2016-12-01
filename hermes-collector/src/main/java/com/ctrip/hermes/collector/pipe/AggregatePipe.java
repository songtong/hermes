package com.ctrip.hermes.collector.pipe;

public abstract class AggregatePipe<T> extends AnalyzePipe<T> implements Aggregate<T> {

	public AggregatePipe(Class<?> clzz) {
		super(clzz);
	}

	@Override
	public final void analyze(T obj) throws Exception {
		//aggregate();
	}

}
