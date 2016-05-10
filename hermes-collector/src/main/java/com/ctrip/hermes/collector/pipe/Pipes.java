package com.ctrip.hermes.collector.pipe;

import java.util.ArrayList;
import java.util.List;

public class Pipes<T> extends Pipe<T> {
	private List<Pipe<T>> m_pipes = new ArrayList<Pipe<T>>();
	
	public Pipes(Class<?> clzz) {
		super(clzz);
	}
	
	public boolean validate(T obj) {
		return obj != null;
	}

	@Override
	public void doProcess(PipeContext context, T obj) {
		for (Pipe<T> p : m_pipes) {
			if (context.isResetEveryPipe()) {
				context.reset();
			}
			p.process(context, obj);
		}
	}
	
	public void addPipe(Pipe<T> pipeline) {
		m_pipes.add(pipeline);
	}
	
	public Pipe<T> getPipe(int index) {
		return m_pipes.get(index);
	}
	
	public int size() {
		return m_pipes.size();
	}
}
