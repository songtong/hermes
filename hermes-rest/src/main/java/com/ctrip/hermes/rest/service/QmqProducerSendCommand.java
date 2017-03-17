package com.ctrip.hermes.rest.service;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import qunar.tc.qmq.Message;
import qunar.tc.qmq.MessageSendStateListener;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.producer.MessageProducerProvider;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.ParserConfig;
import com.ctrip.hermes.rest.service.json.CharSequenceDeserializer;
import com.google.common.util.concurrent.SettableFuture;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixCommandProperties;

public class QmqProducerSendCommand extends HystrixCommand<Future<Message>> {
    private static final ParserConfig parserConfig = new ParserConfig();

    static {
        parserConfig.putDeserializer(CharSequence.class, CharSequenceDeserializer.instance);
    }

    private MessageProducerProvider provider;

    private String topic;

    private Map<String, String> params;

    private InputStream is;

    public QmqProducerSendCommand(MessageProducerProvider provider, String topic, Map<String, String> params, InputStream is) {
        super(Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey(QmqProducerSendCommand.class.getSimpleName()))
                .andCommandKey(HystrixCommandKey.Factory.asKey(topic))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter().withExecutionTimeoutInMilliseconds(5000)));
        this.provider = provider;
        this.topic = topic;
        this.params = params;
        this.is = is;
    }

    @Override
    protected Future<Message> run() throws Exception {
        HashMap<String, Object> attrs = JSON.parseObject(is, HashMap.class);

        Message message = null;
        if (params.containsKey("refKey")) {
            String refKey = params.get("refKey");
            message = provider.generateMessage(refKey, topic);
        }

        if (message == null) {
            message = provider.generateMessage(topic);
        }

        if (params.containsKey("properties")) {
            String properties = params.get("properties");
            for (String pro : properties.split(",")) {
                if (pro.contains("=")) {
                    String[] split = pro.split("=");
                    if (split.length == 2) {
                        message.setProperty(split[0], split[1]);
                    }
                }
            }
        }
        
        attrs.putAll(message.getAttrs());
        ((BaseMessage)message).setAttrs(attrs);
        

        final SettableFuture<Message> sendFuture = SettableFuture.create();
        provider.sendMessage(message, new MessageSendStateListener() {

            @Override
            public void onSuccess(Message message) {
                sendFuture.set(message);
            }

            @Override
            public void onFailed(Message message) {
                sendFuture.setException(new Exception(String.format("Failed to send msg: %s", message)));
            }
        });

        return sendFuture;
    }
    
    
}
