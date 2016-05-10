package com.ctrip.hermes;

public class Hermes {

	public final static String VERSION = "java-0.7.2";

	private static Env m_env;

	public enum Env {
		LOCAL, DEV, FWS, FAT, UAT, LPT, PROD, TOOLS
	}

	public static void initialize(Env env) {
		m_env = env;
	}

	public static Env getEnv() {
		return m_env;
	}

}
