package com.ctrip.hermes.rest;

import java.io.IOException;

import org.apache.curator.test.TestingServer;

public class MockZookeeper {

	private static final int ZK_PORT = 2181;

	private static final String ZK_HOST = "localhost";

	private TestingServer zkTestServer;

	public MockZookeeper() {
		try {
			zkTestServer = new TestingServer(ZK_PORT, false);
			start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void start() {
		try {
			zkTestServer.start();
			System.out.println("embedded zookeeper is up");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void stop() {
		if (zkTestServer != null) {
			try {
				zkTestServer.stop();
				System.out.println("embedded zookeeper is down");
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
	}

	public String getConnectionString() {
		return ZK_HOST + ":" + ZK_PORT;
	}
}
