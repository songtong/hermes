package com.ctrip.hermes;

public class Hermes {
	private static Env m_env;

	public enum Env {
		LOCAL, //
		DEV, //
		FWS, //
		FAT, //
		UAT, //
		LPT, //
		PROD, //
		TOOLS
	}

	public static void initialize(Env env) {
		m_env = env;
	}

	public static Env getEnv() {
		return m_env;
	}
}
