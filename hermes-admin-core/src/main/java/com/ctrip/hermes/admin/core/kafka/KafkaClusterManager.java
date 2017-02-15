package com.ctrip.hermes.admin.core.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.admin.core.model.Idc;
import com.ctrip.hermes.admin.core.service.IdcService;
import com.ctrip.hermes.admin.core.service.StorageService;
import com.ctrip.hermes.core.kafka.KafkaConstants;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.meta.entity.Storage;

@Named
public class KafkaClusterManager {

	@Inject
	private StorageService m_storageService;

	@Inject
	private IdcService m_idcService;

	public KafkaCluster getCluster(String idc) {
		KafkaCluster cluster = new KafkaCluster();
		String bootstrapServersProp = String.format("%s.%s", KafkaConstants.BOOTSTRAP_SERVERS_PROPERTY_NAME, idc);
		String zookeeperConnectProp = String.format("%s.%s", KafkaConstants.ZOOKEEPER_CONNECT_PROPERTY_NAME, idc);
		for (Storage storage : m_storageService.getStorages().values()) {
			if ("kafka".equals(storage.getType())) {
				for (Datasource ds : storage.getDatasources()) {
					Property prop = ds.findProperty(bootstrapServersProp);
					if (prop != null) {
						cluster.setBootstrapServers(prop.getValue());
					}
					prop = ds.findProperty(zookeeperConnectProp);
					if (prop != null) {
						cluster.setZookeeperConnect(prop.getValue());
					}
				}
				break;
			}
		}

		return cluster;
	}

	public Map<String, KafkaCluster> listClusters() throws Exception {
		Map<String, KafkaCluster> clusters = new HashMap<>();
		List<Idc> idcs = m_idcService.listIdcs();
		for (Storage storage : m_storageService.getStorages().values()) {
			if ("kafka".equals(storage.getType())) {
				for (Idc idc : idcs) {
					KafkaCluster cluster = new KafkaCluster();
					cluster.setIdc(idc.getName().toLowerCase());
					String bootstrapServersProp = String.format("%s.%s", KafkaConstants.BOOTSTRAP_SERVERS_PROPERTY_NAME,
					      cluster.getIdc());
					String zookeeperConnectProp = String.format("%s.%s", KafkaConstants.ZOOKEEPER_CONNECT_PROPERTY_NAME,
					      cluster.getIdc());

					for (Datasource ds : storage.getDatasources()) {
						Property prop = ds.findProperty(bootstrapServersProp);
						if (prop != null) {
							cluster.setBootstrapServers(prop.getValue());
						}
						prop = ds.findProperty(zookeeperConnectProp);
						if (prop != null) {
							cluster.setZookeeperConnect(prop.getValue());
						}
					}

					if (cluster.isValid()) {
						clusters.put(cluster.getIdc(), cluster);
					}
				}
			}
		}

		return clusters;
	}
}
