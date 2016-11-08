package com.ctrip.hermes.portal.service.zookeeperMigration;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.curator.test.TestingServer;

public class StartZookeeperEnsemble {

	public static void main(String[] args) throws Exception {
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		Map<Integer, TestingServer> servers = new HashMap<>();
		while (true) {
			String line = in.readLine();

			if ("quit".equals(line)) {
				return;
			} else if (line.startsWith("start")) {
				try {
					int port = new Integer(line.split(" ")[1]);
					if (servers.get(port) != null) {
						System.out.println("Already start");
					} else {
						TestingServer testingServer = new TestingServer(port);
						servers.put(port, testingServer);
						System.out.println(String.format("Start zkEnsemble at port %s success.", port));
					}
				} catch (Exception e) {
					System.out.println("usage: start port");
				}
			} else if (line.startsWith("close")) {
				try {
					int port = new Integer(line.split(" ")[1]);
					TestingServer server = servers.get(port);
					if (server == null) {
						System.out.println("Already close.");
					} else {
						server.close();
						servers.remove(port);
						System.out.println(String.format("Close zkEnsemble at port %s success.", port));
					}

				} catch (Exception e) {
					System.out.println("usage: close port");
				}
			}
		}
	}
}
