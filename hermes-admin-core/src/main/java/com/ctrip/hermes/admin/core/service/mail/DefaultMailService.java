package com.ctrip.hermes.admin.core.service.mail;

import java.util.List;
import java.util.Properties;

import javax.mail.Address;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import javax.mail.internet.MimeUtility;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.utils.StringUtils;

@Named(type = MailService.class)
public class DefaultMailService implements MailService {

	private final static Logger log = LoggerFactory.getLogger(DefaultMailService.class);

	@Inject
	private MailAccountProvider m_account;

	@Override
	public void sendEmail(HermesMail mail) throws Exception {
		if (m_account.getUser() == null) {
			log.warn("Mail accout is not set up correctly, will skip");
			return;
		}

		if (StringUtils.isBlank(mail.getSubject())) {
			throw new IllegalArgumentException("Mail subject can not be blank!");
		}

		if (mail.getReceivers() == null || mail.getReceivers().size() == 0) {
			throw new IllegalArgumentException("Mail receivers can not be empty!");
		}

		Properties properties = new Properties();
		properties.setProperty("mail.transport.protocol", "smtp");
		properties.setProperty("mail.smtp.auth", "true");
		Session session = Session.getInstance(properties);
		session.setDebug(false);

		MimeMessage msg = new MimeMessage(session);
		msg.setFrom(new InternetAddress("\"" + MimeUtility.encodeText("Hermes") + "\"<rdkjmes@ctrip.com>"));
		msg.setSubject(mail.getSubject());
		Multipart bodyMultipart = new MimeMultipart("related");
		msg.setContent(bodyMultipart);
		BodyPart htmlPart = new MimeBodyPart();
		htmlPart.setContent(mail.getBody(), "text/html;charset=utf-8");
		bodyMultipart.addBodyPart(htmlPart);
		msg.saveChanges();

		List<String> receivers = mail.getReceivers();
		Address[] tos = new InternetAddress[receivers.size()];
		for (int i = 0, j = receivers.size(); i < j; i++) {
			tos[i] = new InternetAddress(receivers.get(i));
		}
		msg.setRecipients(Message.RecipientType.TO, tos);

		Transport transport = session.getTransport();
		transport.connect("appmail.sh.ctriptravel.com", 25, m_account.getUser(), m_account.getPassword());
		transport.sendMessage(msg, tos);
		transport.close();
	}

}
