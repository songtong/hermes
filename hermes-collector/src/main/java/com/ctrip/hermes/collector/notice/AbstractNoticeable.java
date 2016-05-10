package com.ctrip.hermes.collector.notice;

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.collector.exception.NoticeException;
import com.ctrip.hermes.collector.notice.annotation.NoticeDetail;
import com.ctrip.hermes.metaservice.service.notify.HermesNotice;
import com.ctrip.hermes.metaservice.service.notify.HermesNoticeContent;
import com.ctrip.hermes.metaservice.service.notify.HermesNoticeType;

public abstract class AbstractNoticeable implements Noticeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractNoticeable.class);
    public static final String DEFAULT_MAIL_ADDRESS = "Rdkjmes@Ctrip.com";
    public static final String DEFAULT_SMS_NUMBER = "13310027139";
    public static final String DEFAULT_TTS_NUMBER = "13310027139";
    
    public HermesNotice get() throws NoticeException {
        HermesNoticeContent content = getNoticeContent();
        
        if (content == null) {
            throw new NoticeException("Notice content is required.");
        }
        
        List<String> receivers= getReceivers();
        
        if (receivers == null || receivers.size() == 0) {
            receivers = getDefaultReceivers(content.getType());
        }
        
        if (receivers == null) {
            throw new NoticeException("Notice receivers are required.");
        }
        
        return new HermesNotice(receivers, content);
    }
    
    protected abstract List<String> getReceivers();
    
    protected HermesNoticeContent getNoticeContent() {
        NoticeDetail detail = this.getClass().getAnnotation(NoticeDetail.class);
        HermesNoticeContent content = null;
        try {
            if (detail != null) {
                content = detail.value().newInstance();
            }
        } catch (InstantiationException | IllegalAccessException e) {
            LOGGER.error("Public default constructor is required for {}, failed to create notice content due to exception: ", this.getClass().getName(), e);
        } 
        return content;
    }

    protected List<String> getDefaultReceivers(HermesNoticeType type) {
        switch (type) {
        case TTS:
            return Collections.singletonList(DEFAULT_TTS_NUMBER);
        case SMS:
            return Collections.singletonList(DEFAULT_SMS_NUMBER);
        case EMAIL:
            return Collections.singletonList(DEFAULT_MAIL_ADDRESS);
        default:
            return null;
        }
    }

}
