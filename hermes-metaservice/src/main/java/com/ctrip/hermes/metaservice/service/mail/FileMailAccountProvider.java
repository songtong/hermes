package com.ctrip.hermes.metaservice.service.mail;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Named;

@Named(type = MailAccountProvider.class)
public class FileMailAccountProvider implements MailAccountProvider, Initializable {

	private static final String FILE = "/opt/ctrip/data/hermes/mail.properties";

	private String m_user;

	private String m_password;

	@Override
	public String getUser() {
		return m_user;
	}

	@Override
	public String getPassword() {
		return m_password;
	}

	@Override
	public void initialize() throws InitializationException {
		Properties p = new Properties();
		File file = new File(FILE);
		if (file.isFile()) {
			try {
				p.load(new FileInputStream(file));
			} catch (Exception e) {
				throw new InitializationException("Error read " + FILE, e);
			}
		}
		m_user = p.getProperty("user");
		m_password = p.getProperty("password");
	}

}
