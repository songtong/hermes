package com.ctrip.hermes.metaservice.service;

import java.util.List;

import com.ctrip.hermes.meta.entity.ZookeeperEnsemble;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface ZookeeperEnsembleService {
	List<ZookeeperEnsemble> listEnsembles();
}
