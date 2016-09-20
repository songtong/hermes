package com.ctrip.hermes.metaservice.service;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.meta.entity.ZookeeperEnsemble;
import com.ctrip.hermes.metaservice.model.ZookeeperEnsembleDao;
import com.ctrip.hermes.metaservice.model.ZookeeperEnsembleEntity;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ZookeeperEnsembleService.class)
public class DefaultZookeeperEnsembleService implements ZookeeperEnsembleService {

	private static final Logger log = LoggerFactory.getLogger(DefaultZookeeperEnsembleService.class);

	@Inject
	private ZookeeperEnsembleDao m_dao;

	private ZookeeperEnsemble toMetaEntity(com.ctrip.hermes.metaservice.model.ZookeeperEnsemble ensembleEntity) {
		ZookeeperEnsemble ensemble = new ZookeeperEnsemble();

		ensemble.setId(ensembleEntity.getName());
		ensemble.setConnectionString(StringUtils.trimToEmpty(ensembleEntity.getConnectionString()));
		ensemble.setIdc(ensembleEntity.getIdc());
		ensemble.setPrimary(ensembleEntity.isPrimary());

		return ensemble;
	}

	@Override
	public List<ZookeeperEnsemble> listEnsembles() {
		List<ZookeeperEnsemble> ensembles = new ArrayList<>();

		try {
			List<com.ctrip.hermes.metaservice.model.ZookeeperEnsemble> ensembleEntities = m_dao
			      .list(ZookeeperEnsembleEntity.READSET_FULL);
			if (ensembleEntities != null && !ensembleEntities.isEmpty()) {
				for (com.ctrip.hermes.metaservice.model.ZookeeperEnsemble ensembleEntity : ensembleEntities) {
					ZookeeperEnsemble ensemble = toMetaEntity(ensembleEntity);
					if (!StringUtils.isEmpty(ensemble.getConnectionString())) {
						ensembles.add(ensemble);
					}
				}
			}
		} catch (DalException e) {
			log.error("Exception occurred while listing zookeeper ensembles", e);
		}

		return ensembles;
	}
}
