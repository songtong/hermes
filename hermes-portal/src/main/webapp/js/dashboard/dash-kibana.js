function k_topic_produce_history(topic, $sce) {
	var url = global_kibana_url
			+ "/#/visualize/edit/Message-Received?embed&_g=(refreshInterval:(display:'10%20seconds',pause:!f,section:1,value:5000),time:(from:now-1h,mode:quick,to:now))&_a=(filters:!(),linked:!f,query:(query_string:(analyze_wildcard:!t,query:'eventType:Message.Received')),vis:(aggs:!((id:'1',params:(),schema:metric,type:count),(id:'3',params:(customInterval:'2h',extended_bounds:(),field:'@timestamp',interval:auto,min_doc_count:1),schema:segment,type:date_histogram),(id:'2',params:(filters:!((input:(query:(query_string:(analyze_wildcard:!t,query:'datas.topic:"
			+ topic
			+ "')))))),schema:group,type:filters)),listeners:(),params:(addLegend:!t,addTimeMarker:!f,addTooltip:!t,defaultYExtents:!f,mode:stacked,scale:linear,setYExtents:!f,shareYAxis:!t,times:!(),yAxis:()),type:histogram))";
	return $sce.trustAsResourceUrl(url);
};

function k_consumer_consume_history(topic, consumer, $sce) {
	var url = global_kibana_url
			+ "/#/visualize/edit/Message-Acked?embed&_g=(refreshInterval:(display:'10%20seconds',pause:!f,section:1,value:10000),time:(from:now-1h,mode:quick,to:now))&_a=(filters:!(),linked:!f,query:(query_string:(analyze_wildcard:!t,query:'eventType:Message.Acked')),vis:(aggs:!((id:'1',params:(),schema:metric,type:count),(id:'2',params:(customInterval:'2h',extended_bounds:(),field:'@timestamp',interval:auto,min_doc_count:1),schema:segment,type:date_histogram),(id:'3',params:(filters:!((input:(query:(query_string:(analyze_wildcard:!t,query:'datas.groupId:"
			+ consumer
			+ "')))))),schema:group,type:filters)),listeners:(),params:(addLegend:!t,addTimeMarker:!f,addTooltip:!t,defaultYExtents:!f,mode:stacked,scale:linear,setYExtents:!f,shareYAxis:!t,times:!(),yAxis:()),type:histogram))";
	return $sce.trustAsResourceUrl(url);
};

function k_consumer_process_history(topic, consumer, $sce) {
	var url = global_kibana_url
			+ "/#/visualize/edit/Process-Time?embed&_g=(refreshInterval:(display:'5%20seconds',pause:!f,section:1,value:5000),time:(from:now-1h,mode:quick,to:now))&_a=(filters:!(),linked:!f,query:(query_string:(analyze_wildcard:!t,query:'*')),vis:(aggs:!((id:'1',params:(field:datas.processTime),schema:metric,type:avg),(id:'2',params:(customInterval:'2h',extended_bounds:(),field:'@timestamp',interval:auto,min_doc_count:1),schema:segment,type:date_histogram),(id:'3',params:(filters:!((input:(query:(query_string:(analyze_wildcard:!t,query:'datas.groupId:"
			+ consumer
			+ "%20AND%20datas.topic:"
			+ topic
			+ "')))))),schema:group,type:filters)),listeners:(),params:(addLegend:!f,addTimeMarker:!f,addTooltip:!t,defaultYExtents:!f,mode:stacked,scale:linear,setYExtents:!f,shareYAxis:!t,times:!(),yAxis:()),type:histogram))";
	return $sce.trustAsResourceUrl(url);
};

function k_top_producer_current(topic, $sce) {
	var url = global_kibana_url
			+ "/#/visualize/edit/Top-Producer?embed&_g=(filters:!(),refreshInterval:(display:'10%20seconds',pause:!f,section:1,value:10000),time:(from:now-1m,mode:relative,to:now))&_a=(filters:!(),linked:!f,query:(query_string:(analyze_wildcard:!t,query:'eventType:Message.Received%20AND%20datas.topic:"
			+ topic
			+ "')),vis:(aggs:!((id:'1',params:(),schema:metric,type:count),(id:'2',params:(field:datas.producerIp.raw,order:desc,orderBy:'1',size:100),schema:bucket,type:terms)),listeners:(),params:(perPage:10,showMeticsAtAllLevels:!f,showPartialRows:!f),type:table))";
	return $sce.trustAsResourceUrl(url);
};

function k_bottom_producer_current(topic, $sce) {
	var url = global_kibana_url
			+ "/#/visualize/edit/Bottom-Producer?embed&_g=(refreshInterval:(display:'10%20seconds',pause:!f,section:1,value:10000),time:(from:now-1m,mode:relative,to:now))&_a=(filters:!(),linked:!f,query:(query_string:(analyze_wildcard:!t,query:'eventType:Message.Received%20AND%20datas.topic:"
			+ topic
			+ "')),vis:(aggs:!((id:'1',params:(),schema:metric,type:count),(id:'2',params:(field:datas.producerIp.raw,order:asc,orderBy:'1',size:100),schema:bucket,type:terms)),listeners:(),params:(perPage:10,showMeticsAtAllLevels:!f,showPartialRows:!f),type:table))";
	return $sce.trustAsResourceUrl(url);
};

function k_top_consumer_current(topic, $sce) {
	var url = global_kibana_url
			+ "/#/visualize/edit/Top-Consumer?embed&_g=(refreshInterval:(display:'10%20seconds',pause:!f,section:1,value:10000),time:(from:now-1m,mode:relative,to:now))&_a=(filters:!(),linked:!f,query:(query_string:(analyze_wildcard:!t,query:'eventType:Message.Acked%20AND%20datas.topic:"
			+ topic
			+ "')),vis:(aggs:!((id:'1',params:(),schema:metric,type:count),(id:'2',params:(field:datas.consumerIp.raw,order:desc,orderBy:'1',size:100),schema:bucket,type:terms)),listeners:(),params:(perPage:10,showMeticsAtAllLevels:!f,showPartialRows:!f),type:table))";
	return $sce.trustAsResourceUrl(url);
};

function k_bottom_consumer_current(topic, $sce) {
	var url = global_kibana_url
			+ "/#/visualize/edit/Bottom-Consumer?embed&_g=(refreshInterval:(display:'10%20seconds',pause:!f,section:1,value:10000),time:(from:now-1m,mode:relative,to:now))&_a=(filters:!(),linked:!f,query:(query_string:(analyze_wildcard:!t,query:'eventType:Message.Acked%20AND%20datas.topic:"
			+ topic
			+ "')),vis:(aggs:!((id:'1',params:(),schema:metric,type:count),(id:'2',params:(field:datas.consumerIp.raw,order:asc,orderBy:'1',size:100),schema:bucket,type:terms)),listeners:(),params:(perPage:10,showMeticsAtAllLevels:!f,showPartialRows:!f),type:table))";
	return $sce.trustAsResourceUrl(url);
};

function k_bottom_process_current(topic, $sce) {
	var url = global_kibana_url
			+ "/#/visualize/edit/Process-Slowest?embed&_g=(refreshInterval:(display:'1%20minute',pause:!f,section:2,value:60000),time:(from:now-1h,mode:quick,to:now))&_a=(filters:!(),linked:!f,query:(query_string:(analyze_wildcard:!t,query:'datas.topic:"
			+ topic
			+ "')),vis:(aggs:!((id:'1',params:(field:datas.processTime),schema:metric,type:avg),(id:'2',params:(field:datas.groupId.raw,order:desc,orderBy:'1',size:10),schema:bucket,type:terms)),listeners:(),params:(perPage:10,showMeticsAtAllLevels:!f,showPartialRows:!f),type:table))";
	return $sce.trustAsResourceUrl(url);
};

function k_max_did_current(topic, $sce) {
	var url = global_kibana_url
			+ "/#/visualize/edit/Max-Delivered-MID?embed&_g=(refreshInterval:(display:'1%20minute',pause:!f,section:2,value:60000),time:(from:now-7d,mode:quick,to:now))&_a=(filters:!(),linked:!f,query:(query_string:(analyze_wildcard:!t,query:'eventType:Message.Delivered%20AND%20datas.topic:"
			+ topic
			+ "')),vis:(aggs:!((id:'1',params:(field:datas.msgId),schema:metric,type:max),(id:'2',params:(field:datas.groupId.raw,order:desc,orderBy:'1',size:5),schema:bucket,type:terms)),listeners:(),params:(perPage:10,showMeticsAtAllLevels:!f,showPartialRows:!f),type:table))";
	return $sce.trustAsResourceUrl(url);
};

function k_max_aid_current(topic, $sce) {
	var url = global_kibana_url
			+ "/#/visualize/edit/Max-Acked-MID?embed&_g=(refreshInterval:(display:'1%20minute',pause:!f,section:2,value:60000),time:(from:now-7d,mode:quick,to:now))&_a=(filters:!(),linked:!f,query:(query_string:(analyze_wildcard:!t,query:'eventType:Message.Acked%20AND%20datas.topic:"
			+ topic
			+ "')),vis:(aggs:!((id:'1',params:(field:datas.msgId),schema:metric,type:max),(id:'2',params:(field:datas.groupId.raw,order:desc,orderBy:'1',size:20),schema:bucket,type:terms)),listeners:(),params:(perPage:10,showMeticsAtAllLevels:!f,showPartialRows:!f),type:table))";
	return $sce.trustAsResourceUrl(url);
};
