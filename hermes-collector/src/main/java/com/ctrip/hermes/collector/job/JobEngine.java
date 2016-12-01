package com.ctrip.hermes.collector.job;

/**
 * @author tenglinxiao
 *
 */
public interface JobEngine {
	// Main entry for jobs kickoff.
	public void bootstrap();
}
