package com.ctrip.hermes.metaserver.fulltest;

import org.unidal.dal.jdbc.DalException;

import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.metaservice.service.MetaService;

public class MockMetaService implements MetaService {

	@Override
	public Meta findLatestMeta() throws DalException {
		Meta meta = null;
		try {
			meta = loadMeta();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return meta;
	}

	protected Meta loadMeta() throws Exception {

		String fileName = MetaServerBaseTest.metaXmlFile;

		return MetaServerBaseTest.MetaHelper.loadMeta(fileName);
	}
}
