var topic_module = angular.module('topic', [ 'ngResource', 'ngRoute', 'smart-table', 'ui.bootstrap', 'lr.upload', 'xeditable' ]).config(
		function($routeProvider) {
			$routeProvider.when('/list/:type', {
				templateUrl : '/jsp/console/topic/topic-list.html',
				controller : 'list-controller'
			}).when('/add/mysql/:type', {
				templateUrl : '/jsp/console/topic/mysql-add.html',
				controller : 'mysql-add-controller'
			}).when('/add/kafka/:type', {
				templateUrl : '/jsp/console/topic/kafka-add.html',
				controller : 'kafka-add-controller'
			}).when('/detail/mysql/:type/:topicName', {
				templateUrl : '/jsp/console/topic/mysql-detail.html',
				controller : 'mysql-detail-controller'
			}).when('/detail/kafka/:type/:topicName', {
				templateUrl : '/jsp/console/topic/kafka-detail.html',
				controller : 'kafka-detail-controller'
			});
		}).service('TopicService', [ '$resource', '$window', '$q', function($resource, $window, $q) {
	var topic_resource = $resource("/api/topics/:name", {}, {
		get_topic_detail : {
			method : 'GET',
			isArray : false
		},
		update_topic : {
			method : 'PUT'
		},
		add_partition : {
			method : 'POST',
			url : '/api/topics/:name/partition/add'
		},
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
				name : '@name',
				force_schema : '@force_schema'
			}
		}
	});

	var meta_resource = $resource('/api/meta/', {}, {
		get_storages : {
			method : 'GET',
			url : '/api/meta/storages',
			isArray : true
		},
		get_endpoints : {
			method : 'GET',
			url : '/api/meta/endpoints',
			isArray : true
		}
	});

	var consumer_resource = $resource('/api/consumers/:topic', {}, {
		get_consumers : {
			method : 'GET',
			isArray : true
		}
	});

	function find_datasource_names(data, type) {
		for (var i = 0; i < data.length; i++) {
			if (data[i].type == type) {
				return collect_schemas(data[i].datasources, 'id', false);
			}
		}
	}

	function find_endpoint_names(data, type) {
		names = [];
		for (var i = 0; i < data.length; i++) {
			if (data[i].type == type) {
				names.push(data[i].id);
			}
		}
		return names;
	}

	var current_topics = [];
	var storages = [];

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
		'update_topic' : function(topic_name, content) {
			var d = $q.defer();
			topic_resource.update_topic({
				name : topic_name
			}, content, function(result) {
				d.resolve(result);
			}, function(result) {
				d.reject(result.data);
			});
			return d.promise;
		},
		'add_partition' : function(topic_name, content) {
			var d = $q.defer();
			topic_resource.add_partition({
				name : topic_name
			}, content, function(result) {
				d.resolve(result);
			}, function(result) {
				d.reject(result.data);
			});
			return d.promise;
		},
		'fetch_storages' : function() {
			var d = $q.defer();
			meta_resource.get_storages({}, function(result) {
				storages = result;
				d.resolve();
			});
			return d.promise;
		},
		'get_datasource_names' : function(type) {
			return find_datasource_names(storages, type);
		},
		'fetch_topic_detail' : function(topic) {
			var d = $q.defer();
			topic_resource.get_topic_detail({
				name : topic
			}, function(query_result) {
				d.resolve(query_result);
			});
			return d.promise;
		},
		'fetch_consumers_for_topic' : function(topic) {
			var d = $q.defer();
			consumer_resource.get_consumers({
				topic : topic
			}, function(result) {
				d.resolve(result);
			});
			return d.promise;
		},
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
		'sync_topic' : function(topic_name, force_sync_schema) {
			topic_resource.sync_topic({
				name : topic_name,
				force_schema : force_sync_schema
			}, function(success_resp) {
				show_op_info.show("同步" + topic_name + "成功!", true);
			}, function(error_resp) {
				show_op_info.show("同步失败: " + error_resp.data, false);
			});
		}
	}
} ]);

topic_module.filter('short', function() {
	return function(input, length) {
		input = input || '';
		length = length || 30;
		input = input.replace(/\\"/g, '"');
		if (input.length <= length) {
			return input;
		}
		out = input.substring(0, length / 2) + " ... " + input.substring(input.length - length / 2);
		return out;
	}
});