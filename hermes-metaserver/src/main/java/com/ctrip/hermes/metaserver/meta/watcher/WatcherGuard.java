package com.ctrip.hermes.metaserver.meta.watcher;

public interface WatcherGuard {

	boolean pass(int version);

	int updateVersion();
	
	int getVersion();

}
