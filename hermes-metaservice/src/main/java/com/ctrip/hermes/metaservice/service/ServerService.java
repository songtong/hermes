package com.ctrip.hermes.metaservice.service;

import java.util.ArrayList;
import java.util.List;

import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.metaservice.converter.ModelToEntityConverter;
import com.ctrip.hermes.metaservice.model.Server;
import com.ctrip.hermes.metaservice.model.ServerDao;
import com.ctrip.hermes.metaservice.model.ServerEntity;

@Named
public class ServerService {

	@Inject
	protected ServerDao m_serverDao;

	public List<Server> findServers() throws DalException {
		return m_serverDao.list(ServerEntity.READSET_FULL);
	}

	public Server findServerByName(String serverName) throws DalException {
		List<Server> servers = m_serverDao.findById(serverName, ServerEntity.READSET_FULL);
		if (servers.isEmpty()) {
			return null;
		}
		return servers.get(0);
	}

	public void addServer(Server server) throws DalException {
		m_serverDao.insert(server);
	}

	public void deleteServer(String serverName) throws DalException {
		Server server = new Server();
		server.setId(serverName);
		m_serverDao.deleteByPK(server);
	}

	public void updateServer(Server server) throws DalException {
		m_serverDao.updateByPK(server, ServerEntity.UPDATESET_FULL);
	}

	public List<com.ctrip.hermes.meta.entity.Server> listServerEntities() throws DalException {
		List<com.ctrip.hermes.meta.entity.Server> serverEntyties = new ArrayList<>();
		for (Server server : m_serverDao.list(ServerEntity.READSET_FULL)) {
			serverEntyties.add(ModelToEntityConverter.convert(server));
		}
		return serverEntyties;
	}
}
