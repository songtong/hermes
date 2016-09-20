var dashtopic = angular.module("dash-topic", [ 'ngResource', 'ui.bootstrap', 'smart-table', 'ngRoute', 'user' ]);

dashtopic.config(function($routeProvider) {
	$routeProvider.when('/detail/:topic/:consumer?', {
		templateUrl : '/jsp/console/dashboard/dash-topic-detail.html',
		controller : 'dash-topic-controller'
	}).when('/latest/:topic', {
		templateUrl : '/jsp/console/dashboard/dash-topic-detail-latest.html',
		controller : 'dash-topic-controller'
	}).when('/deadletter/latest/:topic/:consumer', {
		templateUrl : '/jsp/console/dashboard/dash-topic-deadletter.html',
		controller : 'dash-topic-controller'
	}).when('/consume/:topic/:consumer', {
		templateUrl : '/jsp/console/dashboard/dash-topic-consume.html',
		controller : 'dash-topic-controller'
	}).when('/top/produce/:topic', {
		templateUrl : '/jsp/console/dashboard/dash-topic-top-produce.html',
		controller : 'dash-topic-controller'
	}).when('/top/consume/:topic', {
		templateUrl : '/jsp/console/dashboard/dash-topic-top-consume.html',
		controller : 'dash-topic-controller'
	}).when('/top/process/:topic', {
		templateUrl : '/jsp/console/dashboard/dash-topic-top-process.html',
		controller : 'dash-topic-controller'
	});
});

dashtopic.filter('short', function() {
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

dashtopic.filter('delay', function() {
	return function format_delay(input) {
		input = input || 0;
		if (input >= 86400000) {
			return parseInt(input / 86400000) + "天" + format_delay(input % 86400000);
		}
		if (input >= 3600000) {
			return parseInt(input / 3600000) + "小时" + format_delay(input % 3600000);
		}
		if (input >= 60000) {
			return parseInt(input / 60000) + "分钟" + format_delay(input % 60000);
		}
		return parseInt(input / 1000) + "秒";
	}
});

dashtopic.service("DashboardTopicService", [ '$resource', '$q', function($resource, $q) {
	var current_topic = 'NONE';

	var trifecta_urls = {
		fws : "http://10.2.7.138:8888/",
		uat : "http://10.3.8.63:8888/",
		prod : "http://10.8.82.32:8888/"
	}
	var get_consumers_for_topic = $resource('/api/consumers/:topic');
	var dash_resource = $resource('/api/dashboard', {}, {
		get_topic_delay : {
			method : 'GET',
			url : '/api/dashboard/detail/topics/:topic/delay'
		},
		get_topic_briefs : {
			method : 'GET',
			isArray : true,
			url : '/api/dashboard/brief/topics'
		},
		get_topic_latest_msgs : {
			method : 'GET',
			isArray : true,
			url : '/api/dashboard/topics/:topic/latest'
		},
		get_consumer_delay_for_topic : {
			method : 'GET',
			isArray : true,
			url : '/api/dashboard/:topic/:consumer/delay'
		},
		getLatestDeadletters : {
			method : 'GET',
			isArray : true,
			url : '/api/dashboard/deadletter/latest/:topic/:consumer'
		},
		downloadDeadletters : {
			method : 'GET',
			isArray : true,
			url : '/api/dashboard/deadletter/download/:topic/:consumer',
			params : {
				topic : '@topic',
				consumer : '@consumer',
				timeStart : '@timeStart',
				timeEnd : '@timeEnd'
			}

		}
	});
	return {
		getLatestDeadletters : function(topicName, consumerName) {
			var delay = $q.defer();
			dash_resource.getLatestDeadletters({
				topic : topicName,
				consumer : consumerName
			}, function(result) {
				delay.resolve(result);
			}, function(result) {
				delay.reject(result);
				show_op_info.show("获取consumer死信失败！Error Msg：" + result.data, false)
			});
			return delay.promise;
		},
		downloadDeadletters : function(topicName, consumerName, timeStart, timeEnd) {
			console.log(topicName, consumerName, timeStart, timeEnd);
			dash_resource.downloadDeadletters({
				topic : topicName,
				consumer : consumerName,
				timeStart : timeStart,
				timeEnd : timeEnd
			}, function(result) {
				return result;
			}, function(result) {
				show_op_info.show("下载失败！Error Msg：" + result.data, false)
			});
		},
		get_trifecta_urls : function() {
			return trifecta_urls;
		},
		get_consumer_delay_for_topic : function(topicName, consumerName) {
			var delay = $q.defer();
			dash_resource.get_consumer_delay_for_topic({
				topic : topicName,
				consumer : consumerName
			}, function(result) {
				delay.resolve(result);
			}, function(result) {
				delay.reject(result);
				show_op_info.show("获取consumer延迟信息失败！Error Msg：" + result.data, false)
			});
			return delay.promise;
		},
		get_consumers_for_topic : function(topicName) {
			var delay = $q.defer();
			get_consumers_for_topic.query({
				topic : topicName
			}, function(result) {
				delay.resolve(result);
			}, function(result) {
				delay.reject(result);
				show_op_info.show("获取consumers列表失败！Error Msg：" + result.data, false)
			});
			return delay.promise;
		},
		get_topic_delay : function(topicName) {
			var delay = $q.defer();
			dash_resource.get_topic_delay({
				topic : topicName
			}, function(result) {
				delay.resolve(result);
			}, function(result) {
				delay.reject(result);
			});
			return delay.promise;
		},
		get_topic_briefs : function() {
			var delay = $q.defer();
			dash_resource.get_topic_briefs(function(result) {
				delay.resolve(result);
			}, function(result) {
				delay.reject(result);
				show_op_info.show("获取topic基本信息失败！Error Msg：" + result.data, false);
			});
			return delay.promise;
		},
		get_topic_latest_msgs : function(topicName) {
			var delay = $q.defer();
			dash_resource.get_topic_latest_msgs({
				topic : topicName
			}, function(result) {
				delay.resolve(result);
			}, function(result) {
				delay.reject(result);
				show_op_info.show("获取topic基本信息失败！Error Msg：" + result.data, false);
			});
			return delay.promise;
		},
		get_current_topic : function() {
			return current_topic;
		},
		set_current_topic : function(topic) {
			current_topic = topic;
		}
	}
} ])