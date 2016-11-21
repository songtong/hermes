package com.ctrip.hermes.portal.mail;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.unidal.helper.Reflects;

import com.ctrip.hermes.admin.core.service.ConsumerService;
import com.ctrip.hermes.admin.core.service.TopicService;
import com.ctrip.hermes.admin.core.service.mail.HermesMail;
import com.ctrip.hermes.admin.core.service.mail.MailService;
import com.ctrip.hermes.admin.core.view.ConsumerGroupView;
import com.ctrip.hermes.admin.core.view.SchemaView;
import com.ctrip.hermes.env.ClientEnvironment;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.portal.config.PortalConfig;
import com.ctrip.hermes.portal.service.mail.DefaultPortalMailService;

public class SendUPloadSchemaMailTest {
	private DefaultPortalMailService m_portalMailService;

	@Before
	public void init() throws Exception {
		m_portalMailService = new DefaultPortalMailService();

		MailService mockMailService = mock(MailService.class);
		TopicService mockTopicService = mock(TopicService.class);
		ConsumerService mockConsumerService = mock(ConsumerService.class);
		ClientEnvironment mockEnv = mock(ClientEnvironment.class);
		PortalConfig mockConfig = mock(PortalConfig.class);

		Topic topic = new Topic();
		topic.setName("topicName");
		topic.setStorageType("storageType");
		topic.setEndpointType("endpointType");
		when(mockTopicService.findTopicEntityById(anyLong())).thenReturn(topic);

		List<ConsumerGroupView> cgs = new ArrayList<>();
		ConsumerGroupView cg1 = new ConsumerGroupView();
		cg1.setOwner1("invalid");
		cg1.setOwner2("valid/valid@Ctrip.com");
		ConsumerGroupView cg2 = new ConsumerGroupView();
		cg2.setOwner1("invalid/invalid@Ctrip.com@Ctrip.com");
		cg2.setOwner2("yq/qingyang@Ctrip.com");
		cgs.add(cg1);
		cgs.add(cg2);
		when(mockConsumerService.findConsumerViews(anyLong())).thenReturn(cgs);

		when(mockEnv.getEnv()).thenReturn("FWS");

		when(mockConfig.getHermesEmailGroupAddress()).thenReturn("qingyang@Ctrip.com");
		when(mockConfig.getEmailTemplateDir()).thenReturn("/templates");
		when(mockConfig.getCurrentPortalHost()).thenReturn("http://127.0.0.1:7678");

		doAnswer(new Answer<Void>() {

			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				HermesMail mail = invocation.getArgumentAt(0, HermesMail.class);
				System.out.println(mail.getReceivers());
				System.out.println(mail.getBody());
				return null;
			}
		}).when(mockMailService).sendEmail(any(HermesMail.class));

		Reflects.forField().setDeclaredFieldValue(DefaultPortalMailService.class, "m_mailService", m_portalMailService,
		      mockMailService);
		Reflects.forField().setDeclaredFieldValue(DefaultPortalMailService.class, "m_topicService", m_portalMailService,
		      mockTopicService);
		Reflects.forField().setDeclaredFieldValue(DefaultPortalMailService.class, "m_consumerService",
		      m_portalMailService, mockConsumerService);
		Reflects.forField().setDeclaredFieldValue(DefaultPortalMailService.class, "m_env", m_portalMailService, mockEnv);
		Reflects.forField().setDeclaredFieldValue(DefaultPortalMailService.class, "m_config", m_portalMailService,
		      mockConfig);
		m_portalMailService.initialize();
	}

	@Test
	public void test() {
		SchemaView schema = new SchemaView();
		schema.setName("SchemaName");
		schema.setVersion(1);
		schema.setType("avro");
		schema.setTopicId(69L);
		schema.setId(100L);

		m_portalMailService.sendUploadSchemaMail(schema, "qingyang@Ctrip.com", "qingyang");
	}

}
