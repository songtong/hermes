package com.ctrip.hermes.portal.dal.application;

import java.util.List;

import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

@Named(type = HermesApplicationDao.class)
public class DefaultHermesApplicationDao implements HermesApplicationDao {

	@Inject
	private ApplicationDao m_appDao;


	@Override
	public List<Application> getAppsByTS(int type, int status) throws DalException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Application getAppById(long id) throws DalException {
		return m_appDao.findByPK(id, ApplicationEntity.READSET_FULL);
	}

	@Override
	public long saveApplication(Application application) throws DalException {
		m_appDao.insert(application);
		return application.getId();
	}

	@Override
	public List<Application> getApplicationsByStatus(int status) throws DalException {
		return m_appDao.findByStatus(status, ApplicationEntity.READSET_FULL);
	}

	@Override
	public Application updateApplication(Application application) throws DalException {
		m_appDao.updateByPK(application, ApplicationEntity.UPDATESET_FULL);
		return application;
	}

}
