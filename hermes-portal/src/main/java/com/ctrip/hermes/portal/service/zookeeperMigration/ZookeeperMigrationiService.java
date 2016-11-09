package com.ctrip.hermes.portal.service.zookeeperMigration;

import com.ctrip.hermes.metaservice.model.Server;

public interface ZookeeperMigrationiService {

	public void initializeZkFromBaseMeta(String zkEnsembleId) throws Exception;

	// update metaserver statys:no lease assigning
	public void setAllMetaServersToOutageStatus();

	public void setMetaServerToOutageStatus(Server server);

	public void setAllMetaServersToRunningStatus();

	public void setMetaServerToRunningStatus(Server server);

	public void forceAllMetaServersToUpdateBaseMeta();

	public void forceMetaServerToUpdateBaseMeta(Server server);

	public void checkNoConsumerOrBrokerLeaseOnAllMetaServers();

	public void checkNoConsumerOrBrokerLeaseOnMetaServer(Server server);

	// update primary zk ensemble on baseMeta : to inform brokers
	public void switchPrimaryZkEnsemble(String zkEnsembleId);

	public void pauseAndSwitchSelfZkEnsemble();

	public void pauseAndSwitchAllMetaServersZkEnsemble();

	public void pauseAndSwitchMetaServerZkEnsemble(Server server);

	public void checkSelfIsDisconnectedToZkEnsemble();

	public void checkAllMetaServersAreDisconnectedToZkEnsemble();

	public void checkMetaServerIsDisconnectedToZkEnsemble();

	public void checkSelfIsConnectedToZkEnsemble();

	public void checkAllMetaServersAreConnectedToZkEnsemble();

	public void checkMetaServerIsConnectedToZkEnsemble();

	public void checkSelfZkEnsembleIsPrimary();

	public void checkAllMetaServersZkEnsembleArePrimary();

	public void checkMetaServerZkEnsembleIsPrimary();

	public void getRunningBrokers();

}
