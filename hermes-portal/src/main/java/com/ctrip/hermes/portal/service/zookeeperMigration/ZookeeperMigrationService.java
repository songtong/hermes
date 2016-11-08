package com.ctrip.hermes.portal.service.zookeeperMigration;

import java.util.Map;

import com.ctrip.hermes.metaservice.model.Server;
import com.ctrip.hermes.portal.service.zookeeperMigration.DefaultZookeeperMigrationService.CheckResult;

public interface ZookeeperMigrationService {

	public void initializeZkFromBaseMeta(int zkEnsembleId);

	public Map<String, CheckResult> stopLeaseAssigningAndCheckStatusForAllMetaServers() throws Exception;

	public CheckResult stopLeaseAssigningAndCheckStatus(Server server) throws Exception;

	public Map<String, CheckResult> pauseAndSwitchAllMetaServersZkEnsembleAndCheckStatus() throws Exception;

	public CheckResult pauseAndSwitchMetaServerZkEnsembleAndCheckStatus(Server server) throws Exception;

	public Map<String, CheckResult> resumeAllMetaServersAndCheckStatus() throws Exception;

	public CheckResult resumeMetaServerAndCheckStatus(Server server) throws Exception;
	
	public Map<String, CheckResult> startLeaseAssigningAndCheckStatusForAllMetaServers() throws Exception;
	
	public CheckResult startLeaseAssigningAndCheckStatus(Server server) throws Exception;
	
	public Map<String, CheckResult> getRunningBrokers() throws Exception;

}
