topic_module.run(function(editableOptions) {
	editableOptions.theme = 'bs3';
}).controller('mysql-detail-controller', [ '$scope', '$resource', '$routeParams', 'TopicService', '$q', function(scope, resource, routeParams, TopicService, $q) {
	scope.logined = logined;
	scope.current_topic_type = routeParams['type'];
	scope.topic_name = routeParams['topicName'];

	scope.codec_types = [ 'json', 'cmessaging' ];

	scope.topic = TopicService.fetch_topic_detail(scope.topic_name).then(function(result) {
		scope.topic = result;
		scope.partitionCount = scope.topic.partitions.length;
	});
	scope.consumers = TopicService.fetch_consumers_for_topic(scope.topic_name).then(function(result) {
		scope.consumers = result;
	});

	scope.load_datasource_names = function() {
		return scope.datasource_names ? null : TopicService.fetch_storages().then(function() {
			scope.datasource_names = TopicService.get_datasource_names(scope.topic.storageType);
		});
	}
	scope.load_endpoint_names = function() {
		return scope.endpoint_names ? null : TopicService.fetch_endpoints().then(function() {
			scope.endpoint_names = TopicService.get_endpoint_names(scope.topic.endpointType);
		})
	}

	scope.check_not_null = function(data) {
		if (data == null)
			return "Can not be null!";
	}
	scope.update_topic = function() {
		scope.topic.partitions = scope.topic.partitions.concat(scope.new_partitions);
		bootbox.confirm({
			title : "请确认",
			message : "确认要修改 Topic: <label class='label label-success'>" + scope.topic_name + "</label> 吗？",
			locale : "zh_CN",
			callback : function(result) {
				if (result) {
					document.getElementById("updateButton").disabled = "disabled";
					show_op_info.show("更新中，请稍候......", true);
					console.log(scope.topic.partitions);
					TopicService.update_topic(scope.topic_name, scope.topic).then(function(result) {
						document.getElementById("updateButton").disabled = false;
						show_op_info.show("修改topic ( " + scope.topic_name + ") 成功!", true);
						scope.topic = result;
						scope.new_partitions = [];
						scope.partitionCount = scope.topic.partitions.length;
					}, function(data) {
						document.getElementById("updateButton").disabled = false;
						show_op_info.show("修改topic ( " + scope.topic_name + ")	 失败!" + data, false);
					});
				} else {
					scope.topic = TopicService.fetch_topic_detail(scope.topic_name).then(function(result) {
						scope.topic = result;
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
