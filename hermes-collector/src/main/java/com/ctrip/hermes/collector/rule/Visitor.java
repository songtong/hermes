package com.ctrip.hermes.collector.rule;

import java.util.List;

import com.ctrip.hermes.collector.rule.annotation.Context;
import com.ctrip.hermes.collector.rule.annotation.EPL;
import com.ctrip.hermes.collector.rule.annotation.Pattern;
import com.ctrip.hermes.collector.rule.annotation.Table;
import com.ctrip.hermes.collector.rule.annotation.Window;

public interface Visitor {
	public void visitContexts(List<Context> contexts);
	
	public void visitTables(List<Table> tables);
	
	public void visitWindows(List<Window> windows);
	
	public void visitEPLs(List<EPL> epls);
	
	public void visitPatterns(List<Pattern> patterns);
	
	public void visitTypes(List<Class<?>> events);
	
	public void visitUtils(List<Class<?>> utils);
	
	public void visit(Visitable visitable);
}
