package com.ctrip.hermes.portal.topic;

import java.util.ArrayList;
import java.util.List;

import com.ctrip.hermes.portal.dal.tag.Tag;

public class TopicView extends com.ctrip.hermes.metaservice.view.TopicView {
	private List<Tag> tags = new ArrayList<Tag>();

	public List<Tag> getTags() {
		return tags;
	}

	public void setTags(List<Tag> tags) {
		this.tags = tags;
	}
}
