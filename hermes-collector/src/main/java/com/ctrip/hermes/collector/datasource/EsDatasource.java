package com.ctrip.hermes.collector.datasource;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EsDatasource extends HttpDatasource {
	private static Logger logger = LoggerFactory.getLogger(EsDatasource.class);
	private String m_token = null;
	private String m_tokenFile = null;

	public EsDatasource() {
		super(HttpDatasourceType.ES);
	}
	
	public String getToken() {
		return m_token;
	}

	public void setToken(String token) {
		m_token = token;
	}

	public String getTokenFile() {
		return m_tokenFile;
	}

	public void setTokenFile(String tokenFile) {
		m_tokenFile = tokenFile;
	}

	public void resolveDependency() {
		if (m_token == null) {
			BufferedReader reader = null;
			try {
				StringBuilder builder = new StringBuilder();
				reader = new BufferedReader(new InputStreamReader(new FileInputStream("/opt/ctrip/data/hermes/hermes-es.token")));
				String line = null; 
				while ((line = reader.readLine()) != null) {
					builder.append(line.trim());
				}
				reader.close();
				m_token = builder.toString();
			} catch (Exception e) {
				logger.error(String.format("Failed to load es token file: %s", m_tokenFile), e);
			} finally {
				if (reader != null) {
					try {
						reader.close();
					} catch (IOException e) {
						// ignore.
					}
				}
			}
		}
	}
	
	public static EsDatasource newEsDatasource(String api) {
		EsDatasource datasource = new EsDatasource();
		datasource.setApi(api);
		return datasource;
	}
	
}
