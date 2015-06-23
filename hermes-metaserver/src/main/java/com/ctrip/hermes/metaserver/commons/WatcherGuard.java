package com.ctrip.hermes.metaserver.commons;

public interface WatcherGuard {

	boolean pass(int version);

	int updateVersion();
	
	int getVersion();

}
