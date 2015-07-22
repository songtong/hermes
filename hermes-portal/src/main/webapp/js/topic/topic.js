var topic_module = angular.module('topic', [ 'ngResource', 'ngRoute', 'smart-table', 'ui.bootstrap', 'lr.upload' ]).config(function($routeProvider) {
	$routeProvider.when('/list/:type', {
		templateUrl : '/jsp/console/topic/topic-list.html',
		controller : 'list-controller'
	}).when('/add/mysql', {
		templateUrl : '/jsp/console/topic/mysql-add.html',
		controller : 'mysql-add-controller'
	}).when('/add/kafka', {
		templateUrl : '/jsp/console/topic/kafka-add.html',
		controller : 'kafka-add-controller'
	});
}).service('TopicService', [ '$resource', '$window', function($resource, $window) {
	var topic_resource = $resource("/api/topics/:name", {}, {
		deploy_topic : {
			method : 'POST',
			isArray : false,
			url : '/api/topics/:name/deploy',
			params : {
				name : '@name'
			}
		},
		undeploy_topic : {
			method : 'POST',
			isArray : false,
			url : '/api/topics/:name/undeploy',
			params : {
				name : '@name'
			}
		},
		sync_topic : {
			method : 'POST',
			isArray : false,
			url : '/api/topics/:name/sync',
			params : {
				name : '@name'
			}
		}
	});
	var current_topics = [];

	function remove_topic_in_meta(topic, index) {
		topic_resource.remove({
			"name" : topic.name
		}, function(remove_result) {
			show_op_info.show("删除 " + topic.name + " 成功！", true);
			current_topics.splice(index, 1);
		}, function(error_result) {
			show_op_info.show("删除 " + topic.name + " 失败: " + error_result.data, false);
		});
	}

	return {
		'fetch_topics' : function(type) {
			topic_resource.query({
				'type' : type
			}, function(data) {
				current_topics = data;
			});
		},
		'get_topics' : function() {
			return current_topics;
		},
		'save_topic' : function(new_topic, path_when_success) {
			topic_resource.save(new_topic, function(save_result) {
				if (new_topic.storageType == 'kafka') {
					show_op_info.show("保存Topic成功，正在发布Topic至Kafka...", true);
					topic_resource.deploy_topic({}, {
						"name" : new_topic.name
					}, function(response) {
						show_op_info.show("发布Topic到Kafka成功!", true);
						$window.history.back();
					}, function(response) {
						console.log(response.data);
						topic_resource.remove({
							name : new_topic.name
						}, function(success_resp) {
							show_op_info.show("发布Topic到Kafka失败, 删除脏数据成功!", false);
						}, function(error_resp) {
							show_op_info.show("发布Topic到Kafka失败, 删除脏数据失败! " + error_resp.data, false);
						});
					});
				} else {
					show_op_info.show("新增 " + new_topic.name + " 成功！", true);
					$window.history.back()
				}
			}, function(error_result) {
				show_op_info.show("新增 " + new_topic.name + " 失败: " + error_result.data, false);
			});
		},
		'delete_topic' : function(topic, index) {
			if (topic.storageType == 'kafka') {
				topic_resource.undeploy_topic({
					name : topic.name
				}, function(success_resp) {
					show_op_info.show("删除Kafka记录成功, 正在删除Topic...", true);
					remove_topic_in_meta(topic, index);
				}, function(error_resp) {
					show_op_info.show("删除Kafka记录失败", false);
					bootbox.confirm({
						title : "请确认",
						message : "删除Kafka记录失败，是否继续删除Topic?",
						locale : "zh_CN",
						callback : function(result) {
							if (result) {
								remove_topic_in_meta(topic, index);
							}
						}
					});
				});
			} else {
				remove_topic_in_meta(topic, index);
			}
		},
		'sync_topic' : function(topic_name) {
			topic_resource.sync_topic({
				name : topic_name
			}, function(success_resp) {
				show_op_info.show("同步" + topic_name + "成功!", true);
			}, function(error_resp) {
				show_op_info.show("同步失败: " + error_resp.data, false);
			});
		}
	}
} ]);