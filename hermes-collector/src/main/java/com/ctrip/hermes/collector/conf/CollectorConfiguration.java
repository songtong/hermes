package com.ctrip.hermes.collector.conf;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource(value="classpath:hermes-collector.properties")
public class CollectorConfiguration {
	@Value("${datahub.queue.size:1000}")
	private int m_datahubQueueSize;
	
	@Value("${datahub.threads.count:1000}")
	private int m_datahubThreadsCount;
	
	@Value("${datahub.retry.waittime:3000}")
	private long m_retryWaitTime;
	
	@Value("${storage.queue.size:1000}")
	private int m_storageQueueSize;
	
	@Value("${storage.threads.count:1000}")
	private int m_storageThreadsCount;
	
	@Value("${http.es.name}")
	private String m_esDatsourceName;
	
	@Value("${http.es.url}")
	private String m_esDatasourceApi;
	
	@Value("${http.es.token}")
	private String m_esTokenFile;
	
	@Value("${statehub.queue.size:1000}")
	private int m_statehubQueueSize;

	public int getDatahubQueueSize() {
		return m_datahubQueueSize;
	}

	public void setDatahubQueueSize(int datahubQueueSize) {
		m_datahubQueueSize = datahubQueueSize;
	}

	public int getDatahubThreadsCount() {
		return m_datahubThreadsCount;
	}

	public void setDatahubThreadsCount(int datahubThreadsCount) {
		m_datahubThreadsCount = datahubThreadsCount;
	}

	public long getRetryWaitTime() {
		return m_retryWaitTime;
	}

	public void setRetryWaitTime(long retryWaitTime) {
		m_retryWaitTime = retryWaitTime;
	}

	public int getStorageQueueSize() {
		return m_storageQueueSize;
	}

	public void setStorageQueueSize(int storageQueueSize) {
		m_storageQueueSize = storageQueueSize;
	}

	public int getStorageThreadsCount() {
		return m_storageThreadsCount;
	}

	public void setStorageThreadsCount(int storageThreadsCount) {
		m_storageThreadsCount = storageThreadsCount;
	}

	public String getEsDatsourceName() {
		return m_esDatsourceName;
	}

	public void setEsDatsourceName(String esDatsourceName) {
		m_esDatsourceName = esDatsourceName;
	}

	public String getEsDatasourceApi() {
		return m_esDatasourceApi;
	}

	public void setEsDatasourceApi(String esDatasourceApi) {
		m_esDatasourceApi = esDatasourceApi;
	}

	public String getEsTokenFile() {
		return m_esTokenFile;
	}

	public void setEsTokenFile(String esTokenFile) {
		m_esTokenFile = esTokenFile;
	}

	public int getStatehubQueueSize() {
		return m_statehubQueueSize;
	}

	public void setStatehubQueuesize(int statehubQueueSize) {
		m_statehubQueueSize = statehubQueueSize;
	}
	
	
}
