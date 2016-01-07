application_module.controller('app-approval-detail-controller', [ '$scope', '$routeParams', '$resource', 'ApplicationService', '$location', '$window', function($scope, $routeParams, $resource, ApplicationService, $location, $window) {
	var topic_resource = $resource("/api/topics/:name", {}, {
		deploy_topic : {
			method : 'POST',
			isArray : false,
			url : '/api/topics/:name/deploy',
			params : {
				name : '@name'
			}
		}
	});
	var meta_resource = $resource('/api/meta/storages', {}, {
		'get_storage' : {
			method : 'GET',
			isArray : true,
		}
	});
	$scope.order_opts = [ true, false ];
	$scope.datasources = [];
	$scope.new_comment = "";
	ApplicationService.get_generated_application($routeParams['id']).then(function(result) {
		$scope.application = result["key"];
		$scope.view = result["value"];
		updatePageType($scope.application["type"]);
		if ($scope.application["type"] == 0 || $scope.application["type"] == 2) {// create
			$scope.defaultReadDS = $scope.view.partitions[0].readDatasource;
			$scope.defaultWriteDS = $scope.view.partitions[0].writeDatasource;
			if ($scope.view["storageType"] == "mysql") {
				meta_resource.get_storage({
					'type' : 'mysql'
				}, function(data) {
					for (var i = 0; i < data[0].datasources.length; i++) {
						$scope.datasources.push(data[0].datasources[i].id);
					}
					for (var i = 0; i < $scope.view.partitions.length; i++) {
						$scope.view.partitions[i].readDatasource = $scope.defaultReadDS;
						$scope.view.partitions[i].writeDatasource = $scope.defaultWriteDS;
					}
				});
				$scope.codec_types = [ 'json', 'cmessaging' ];
				$scope.endpoint_types = [ 'broker' ];

			} else {
				meta_resource.get_storage({
					'type' : 'kafka'
				}, function(data) {
					for (var i = 0; i < data[0].datasources.length; i++) {
						$scope.datasources.push(data[0].datasources[i].id);
					}
					for (var i = 0; i < $scope.view.partitions.length; i++) {
						$scope.view.partitions[i].readDatasource = $scope.defaultReadDS;
						$scope.view.partitions[i].writeDatasource = $scope.defaultWriteDS;
					}
				});
				$scope.properties = [ 'partitions', 'replication-factor', 'retention.bytes', 'retention.ms' ]
				$scope.endpoint_types = [ 'kafka', 'broker' ];
				$scope.codec_types = [ 'avro', 'json' ];
			}
		}
	});

	$scope.add_partition = function() {
		var new_partition = {};
		new_partition.readDatasource = $scope.defaultReadDS;
		new_partition.writeDatasource = $scope.defaultWriteDS;
		$scope.view.partitions.push(new_partition);
	};

	$scope.delete_partition = function(index) {
		$scope.view.partitions.splice(index, 1);
	};

	$scope.add_property = function() {
		var new_property = {};
		$scope.view.properties.push(new_property);
	};

	$scope.delete_property = function(index) {
		$scope.view.properties.splice(index, 1);
	};

	function updatePageType(typeCode) {
		switch (typeCode) {
		case 0:
			$scope.currentPageType = "Topic创建";
			break;
		case 1:
			$scope.currentPageType = "Consumer创建";
			break;
		case 2:
			$scope.currentPageType = "Topic修改";
			break;
		case 3:
			$scope.currentPageType = "Consumer修改";
			break;
		default:
			console.log("default");
		}
	}

	function get_new_consuemr(view, topics, i) {
		var consumer = {};
		consumer.topicName = topics[i];
		consumer.orderedConsume = view.orderedConsume;
		consumer.appId = view.appId;
		consumer.groupName = view.groupName;
		consumer.retryPolicy = view.retryPolicy;
		consumer.ackTimeoutSeconds = view.ackTimeoutSeconds;
		consumer.owner = view.owner;
		return consumer;
	}

	$scope.permit_topic_application = function() {
		document.getElementById("permitTopicButton").disabled = "disabled";
		show_op_info.show("正在创建Topic...", true);
		topic_resource.save($scope.view, function(save_result) {
			if ($scope.view.storageType == 'kafka') {
				show_op_info.show("保存Topic成功，正在发布Topic至Kafka...", true);
				topic_resource.deploy_topic({}, {
					"name" : $scope.view.name
				}, function(response) {
					ApplicationService.pass_application($scope.application.id, $scope.new_comment, "Hermes").then(function(result) {
						$window["location"].replace("/console/topic#detail/kafka/kafka/" + $scope.view.name);
						show_op_info.show("发布Topic到Kafka成功!表单状态修改成功！", true);
					}, function(result) {
						show_op_info.show("发布Topic到Kafka成功!表单状态修改失败！", false);
					});
				}, function(response) {
					console.log(response.data);
					topic_resource.remove({
						name : $scope.view.name
					}, function(success_resp) {
						show_op_info.show("发布Topic到Kafka失败, 删除脏数据成功!", false);
					}, function(error_resp) {
						show_op_info.show("发布Topic到Kafka失败, 删除脏数据失败! " + error_resp.data, false);
					});
				});
			} else {
				ApplicationService.pass_application($scope.application.id, $scope.new_comment, "Hermes").then(function(result) {
					$window["location"].replace("/console/topic#detail/mysql/mysql/" + $scope.view.name);
					show_op_info.show("Topic创建成功！表单状态修改成功", true);
				}, function(result) {
					show_op_info.show("Topic创建成功！表单状态修改失败", false);
				});
			}
		}, function(error_result) {
			show_op_info.show("新增 " + $scope.view.name + " 失败: " + error_result.data, false);
			document.getElementById("permitButton").disabled = false;
		});
	}
	$scope.permit_consumer_application = function() {
		document.getElementById("permitConsumerButton").disabled = "disabled";
		var topics = $scope.view.topicName.split(",");
		console.log(topics);
		show_op_info.show("正在创建Consumer...", true);
		var consumers = [];
		for (var i = 0; i < topics.length; i++) {
			consumers.push(get_new_consuemr($scope.view, topics, i));
		}
		ApplicationService.add_consumers(consumers).then(function(result1) {
			ApplicationService.pass_application($scope.application.id, $scope.new_comment, "Hermes").then(function(result) {
				$scope.application = result;
				show_op_info.show("为Topic：" + result1.success_topics + "创建Consumer成功！(共"+result1.success_topics.length+"个成功，"+(topics.length-result1.success_topics.length)+"个失败)，表单状态修改成功", true);
			}, function(result) {
				show_op_info.show("为Topic：" + result1.success_topics + "创建Consumer成功！(共"+result1.success_topics.length+"个成功，"+(topics.length-result1.success_topics.length)+"个失败)，表单状态修改失败", false);
			});

		}, function(result) {
			show_op_info.show("新增 consumer失败! " + error_result.data, false);
			document.getElementById("permitConsumerButton").disabled = false;
		});
	}

	$scope.reject_application = function() {
		document.getElementById("rejectButton").disabled = "disabled";
		console.log($scope.application.comment);
		ApplicationService.reject_application($scope.application.id, $scope.new_comment, "Hermes").then(function(result) {
			show_op_info.show("拒绝申请成功", true);
			document.getElementById("rejectButton").disabled = false;
			$scope.application = result;
		}, function(result) {
			document.getElementById("rejectButton").disabled = false;
			show_op_info.show("拒绝申请失败", false);
		});
	}
} ])