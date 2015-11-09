package com.ctrip.hermes.portal.dal.application;

import java.util.List;

import org.unidal.dal.jdbc.DalException;

public interface HermesApplicationDao {

	public List<Application> getAppsByTS(int type, int status) throws DalException;

	public Application getAppById(long id) throws DalException;

	public long saveApplication(Application application) throws DalException;

	public List<Application> getApplicationsByStatus(int status) throws DalException;
}
