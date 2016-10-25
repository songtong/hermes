topic_module.run(function(editableOptions) {
	editableOptions.theme = 'bs3';
}).controller('kafka-detail-controller', [ '$scope', '$resource', '$routeParams', 'TopicService', '$q', 'user', function(scope, resource, routeParams, TopicService, $q, user) {
	producer_monitor_config_resource = resource('/api/monitor/config/topic/:topic', {}, {
		'set_producer_monitor_config' : {
			method : 'POST',
			params : {
				topic : '@topic',
				ssoUser : '@ssoUser',
				ssoMail : '@ssoMail'
			}
		},
		'get_producer_monitor_config' : {
			method : 'GET',
			isArray : false
		}
	});
	
	scope.logined = user.sn;
	scope.current_topic_type = routeParams['type'];
	scope.topic_name = routeParams['topicName'];

	scope.codec_types = [ 'avro', 'json' ];
	scope.endpoint_types = [ 'kafka', 'broker' ];
	scope.Ops = [ true, false ];
	scope.compressionTypes = [ 'gzip', 'deflater' ];
	
	scope.switch_statuses = [ {
		value : true,
		text : 'true'
	}, {
		value : false,
		text : 'false'
	} ];

	scope.topic = TopicService.fetch_topic_detail(scope.topic_name).then(function(result) {
		scope.topic = result;
		decodeCodec(scope.topic);
		scope.partitionCount = scope.topic.partitions.length;
		
		// Fetch producer monitor config.
		producer_monitor_config_resource.get_producer_monitor_config({
			topic: scope.topic.name
		}).$promise.then(function(result) {
			scope.currentProducerMonitorConfig = result;
		});
	});
	
	scope.update_producer_monitor_config = function() {
		bootbox.confirm({
			title : "确认",
			message : "确认要更新报警配置？",
			locale : "zh_CN",
			callback : function(result) {
				if (result) {
					producer_monitor_config_resource.set_producer_monitor_config({
						topic : scope.topic.name,
						ssoUser : ssoUser,
						ssoMail : ssoMail
					}, JSON.stringify(scope.currentProducerMonitorConfig), function(result) {
						show_op_info.show("修改 topic " + scope.currentProducerMonitorConfig.topic + " 监控配置成功!", true);
					}, function(result) {
						show_op_info.show("修改 topic " + scope.currentProducerMonitorConfig.topic + " 监控配置失败! " + result.data, false);
					});
				}
			}
		});
	}
	
	scope.consumers = TopicService.fetch_consumers_for_topic(scope.topic_name).then(function(result) {
		scope.consumers = result;
	});
	
	scope.load_broker_groups = function() {
		return scope.broker_groups ? scope.broker_groups : TopicService.get_broker_groups().then(function(result) {
			scope.broker_groups = result;
		})
	};

	scope.load_datasource_names = function() {
		return scope.datasource_names ? null : TopicService.fetch_storages().then(function() {
			scope.datasource_names = TopicService.get_datasource_names(scope.topic.storageType);
		});
	}
	scope.update_topic = function() {
		scope.topic.partitions = scope.topic.partitions.concat(scope.new_partitions);
		bootbox.confirm({
			title : "请确认",
			message : "确认要修改 Topic: <label class='label label-success'>" + scope.topic_name + "</label> 吗？",
			locale : "zh_CN",
			callback : function(result) {
				if (result) {
					encodeCodec(scope.topic);
					document.getElementById("updateButton").disabled = "disabled";
					show_op_info.show("更新中，请稍候......", true);
					TopicService.update_topic(scope.topic_name, scope.topic).then(function(result) {
						document.getElementById("updateButton").disabled = false;
						show_op_info.show("修改topic ( " + scope.topic_name + " ) 成功!", true);
						scope.topic = result;
						decodeCodec(scope.topic);
						scope.new_partitions = [];
						scope.partitionCount = scope.topic.partitions.length;
					}, function(data) {
						document.getElementById("updateButton").disabled = false;
						show_op_info.show("修改topic ( " + scope.topic_name + " ) 失败!" + data, false);
					});
				} else {
					scope.topic = TopicService.fetch_topic_detail(scope.topic_name).then(function(result) {
						scope.topic = result;
						decodeCodec(scope.topic);
					});
				}
			}
		});
	}
	scope.new_partitions = [];
	scope.add_partition = function() {
		scope.load_datasource_names();
		scope.inserted = {
			id : -1,
			readDatasource : null,
			writeDatasource : null
		}

		scope.new_partitions.push(scope.inserted);
	};

	scope.save_partition = function(data, index) {
		bootbox.confirm({
			title : "请确认",
			message : "确认要新增 Partition吗？",
			locale : "zh_CN",
			callback : function(result) {
				if (result) {
					TopicService.add_partition(scope.topic_name, data).then(function(result) {
						show_op_info.show("保存partition成功！", true);
						scope.topic = result;
					}, function(data) {
						show_op_info.show("保存partition失败！" + data, false);

					});
				} else {
					scope.topic = TopicService.fetch_topic_detail(scope.topic_name).then(function(result) {
						scope.topic = result;
					});
				}
			}
		});
	}

	scope.remove_partition = function(index) {
		scope.new_partitions.splice(index, 1);
	};

} ]);
