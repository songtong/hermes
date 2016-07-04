var consumer_module = angular.module('consumer_module', [ 'ngResource', 'ngRoute', 'ui.bootstrap', 'smart-table', 'xeditable', 'utils', 'components', 'user' ]);

consumer_module.run(function(editableOptions) {
	editableOptions.theme = 'bs3';
}).config(function($routeProvider) {
	$routeProvider.when('/list', {
		templateUrl : '/jsp/console/consumer/consumer-list.html',
		controller : 'consumer-controller'
	}).when('/detail/:topic/:consumer', {
		templateUrl : '/jsp/console/consumer/consumer-detail.html',
		controller : 'consumer-controller'
	});
}).controller('consumer-controller', [ '$scope', '$filter', '$resource', 'clone', '$routeParams', 'user', function(scope, filter, resource, clone, $routeParams, user) {
	consumer_resource = resource('/api/consumers/:topic/:consumer', {}, {
		'add_consumer' : {
			method : 'POST',
			url : '/api/consumers/'
		},
		'update_consumer' : {
			method : 'PUT',
			url : '/api/consumers/'
		},
		'reset_offset' : {
			method : 'POST',
			url : '/api/consumers/:topic/:consumer/offset',
			params : {
				topic : '@topic',
				consumer : '@consumer',
				timestamp : '@timestamp'
			}
		}
	});

	consumer_monitor_config_resource = resource('/api/monitor/config/consumer/:topic/:consumer', {}, {
		'set_consumer_monitor_config' : {
			method : 'POST',
			params : {
				topic : '@topic',
				consumer : '@consumer',
				ssoUser : '@ssoUser',
				ssoMail : '@ssoMail'
			}
		},
		'query' : {
			method : 'GET',
			isArray : false
		}
	});

	meta_resource = resource('/api/', {}, {
		'get_topic_names' : {
			method : 'GET',
			isArray : true,
			url : '/api/topics/names'
		}
	});

	function post_get_consumer_configs(config_result) {
		scope.currentConsumerMonitorConfig = config_result;
		scope.currentConsumerMonitorReceivers = [];
		if (scope.currentConsumerMonitorConfig.alarmReceivers != undefined && scope.currentConsumerMonitorConfig.alarmReceivers.length > 0) {
			scope.currentConsumerMonitorReceivers = $.parseJSON(scope.currentConsumerMonitorConfig.alarmReceivers);
		}
	}

	consumer_resource.query().$promise.then(function(result) {
		scope.consumers = result;
		scope.$broadcast('initialized');
		if ($routeParams['topic'] != null && $routeParams['consumer'] != null) {
			scope.currentConsumer = scope.findConsumer($routeParams['topic'], $routeParams['consumer'], scope.consumers);
			scope.currentConsumer.resetOption = 'latest';
			console.log(scope.currentConsumer);

			consumer_monitor_config_resource.query({
				topic : $routeParams['topic'],
				consumer : $routeParams['consumer']
			}).$promise.then(function(config_result) {
				post_get_consumer_configs(config_result);
			});
		}
	});

	scope.add_receiver = function() {
		scope.currentConsumerMonitorReceivers.push({
			"phone" : "",
			"email" : ""
		});
	};

	scope.remove_receiver = function(index) {
		scope.currentConsumerMonitorReceivers.splice(index, 1);
	};

	scope.findConsumer = function(topic, consumer, consumers) {
		for (var i = 0; i < consumers.length; i++) {
			if (consumers[i].name == consumer && consumers[i].topicName == topic)
				return consumers[i];
		}
		return null;
	}
	scope.logined = user.admin;

	scope.newConsumer = {
		orderedConsume : true
	};

	scope.newTopicNames = "";

	scope.currentTimestamp = new Date();

	scope.maxTimestamp = scope.currentTimestamp.format('%y-%M-%dT%H:%m:%s');

	scope.order_opts = [ true, false ];

	meta_resource.get_topic_names({}, function(result) {
		var result = new Bloodhound({
			local : result,
			datumTokenizer : Bloodhound.tokenizers.whitespace,
			queryTokenizer : Bloodhound.tokenizers.whitespace,
		});

		$('#inputTopicName').on(('tokenfield:createdtoken'), function(e) {
			var topicList = $('#inputTopicName').tokenfield('getTokens');
			var idx;
			for (idx = 0; idx < (topicList.length - 1); idx++) {
				if (e.attrs.value == topicList[idx].value) {
					topicList.pop();
					$('#inputTopicName').tokenfield('setTokens', topicList);
				}
			}
			for (idx = 0; idx < result.local.length; idx++) {
				if (e.attrs.value == result.local[idx]) {
					break;
				}
			}
			if (idx == result.local.length) {
				topicList.pop();
				$('#inputTopicName').tokenfield('setTokens', topicList);
			}
		}).tokenfield({
			typeahead : [ {
				hint : true,
				highlight : true,
				minLength : 1
			}, {
				name : 'topics',
				source : result
			} ],
			beautify : false
		});
	});

	scope.submitReset = function() {
		if (scope.currentConsumer.resetOption == "earliest") {
			scope.currentTimestamp = new Date(0);
		} else if (scope.currentConsumer.resetOption == "latest") {
			scope.currentTimestamp = new Date();
		}
		bootbox.confirm({
			title : "请确认",
			message : "确认要重置offset至 " + scope.currentConsumer.resetOption + " " + (scope.currentConsumer.resetOption == "timepoint" ? scope.currentTimestamp.format('%y-%M-%dT%H:%m:%s') : "") + "吗？",
			locale : "zh_CN",
			callback : function(result) {
				if (result) {
					consumer_resource.reset_offset({
						topic : scope.currentConsumer.topicName,
						consumer : scope.currentConsumer.name,
						timestamp : scope.currentTimestamp.getTime()
					}, function(result) {
						scope.$broadcast('alert-success', 'alert', '重置Offset成功！可以启动consumer。');
					}, function(result) {
						scope.$broadcast('alert-error', 'alert', '重置Offset失败！' + result.data);
					})
				}
			}
		});
	}

	scope.expandCollapse = function() {
		if (scope.currentConsumer.resetOption == 'timepoint') {
			scope.maxTimestamp = new Date().format('%y-%M-%dT%H:%m:%s');
			$('#timepoint').removeClass('collapse');

		} else {
			$('#timepoint').addClass('collapse');
		}
	}

	scope.addConsumer = function(newConsumer) {
		var topics = scope.newTopicNames.split(",");
		for (var i = 0; i < topics.length; i++) {
			var consumer = clone(newConsumer);
			consumer.topicName = topics[i];

			(function(consumer) {
				consumer_resource.add_consumer({}, consumer, function(result) {
					consumer_resource.query().$promise.then(function(result) {
						this.consumers = result
						show_op_info.show("新增 consumer " + consumer.name + " for topic (" + consumer.topicName + ") 成功!", true);
					});
				}, function(result) {
					show_op_info.show("新增 consumer " + consumer.name + " for topics (" + consumer.topicName + ") 失败! " + result.data, false);
				});
			})(consumer);
		}
	};

	scope.update_consumer = function update_consumer(data, topicName, name) {
		data.name = name;
		data.topicName = topicName;
		consumer_resource.update_consumer({}, data, function(save_result) {
			consumer_resource.query().$promise.then(function(result) {
				scope.consumers = result;
				show_op_info.show("修改 consumer " + data.name + " for topic (" + data.topicName + ") 成功!", true);
			});
		}, function(result) {
			show_op_info.show("修改 consumer " + data.name + " for topic (" + data.topicName + ") 失败! " + result.data, false);
		});

	};

	scope.switch_statuses = [ {
		value : true,
		text : 'true'
	}, {
		value : false,
		text : 'false'
	} ];

	scope.update_consumer_monitor_config = function update_consumer_monitor_config(data, receivers, topic, consumer) {
		bootbox.confirm({
			title : "请确认",
			message : "确认要修改 " + consumer + " 的告警配置吗？",
			locale : "zh_CN",
			callback : function(result) {
				if (result) {
					cleanedReceivers = [];
					data.topic = topic;
					data.consumer = consumer;
					for (var idx = 0; idx < receivers.length; idx++) {
						var receiver = receivers[idx];
						if (receiver['email'].trim().length > 0 || receiver['phone'].trim().length > 0) {
							cleanedReceivers.push(receiver);
						}
					}
					data.alarmReceivers = angular.toJson(cleanedReceivers);
					consumer_monitor_config_resource.set_consumer_monitor_config({
						topic : topic,
						consumer : consumer,
						ssoUser : ssoUser,
						ssoMail : ssoMail
					}, data, function(save_result) {
						show_op_info.show("修改 consumer " + data.consumer + " 监控配置成功!", true);
					}, function(result) {
						show_op_info.show("修改 consumer " + data.consumer + " 监控配置失败! " + result.data, false);
					});
				} else {
					consumer_monitor_config_resource.query({
						topic : topic,
						consumer : consumer
					}, function(query_result) {
						post_get_consumer_configs(query_result);
					});
				}
			}
		});
	}

	scope.confirmDeletion = function(topicName, name) {
		scope.consumerName = name;
		scope.$broadcast('confirm', 'deletionConfirm', {
			topic : topicName,
			consumer : name
		});
	};

	scope.deleteConsumer = function(context) {
		consumer_resource.remove({
			topic : context.topic,
			consumer : context.consumer
		}, function(remove_result) {
			consumer_resource.query({}, function(result) {
				scope.consumers = result;
				show_op_info.show("删除成功: " + name + "(" + topicName + ")", true);
			});
		}, function(result) {
			show_op_info.show("删除失败: " + context.consumer + "(" + context.topic + "), " + result.data, false);
		});
	};

} ]);
