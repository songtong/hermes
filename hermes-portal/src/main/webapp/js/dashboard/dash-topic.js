var dashtopic = angular.module("dash-topic", [ 'ngResource', 'ui.bootstrap', 'smart-table', 'ngRoute' ]);

dashtopic.config(function($routeProvider) {
	$routeProvider.when('/detail/:topic', {
		templateUrl : '/jsp/console/dashboard/dash-topic-detail.html',
		controller : 'dash-topic-controller'
	}).when('/latest/:topic', {
		templateUrl : '/jsp/console/dashboard/dash-topic-detail-latest.html',
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

dashtopic.service("DashboardTopicService", function() {
	var current_topic = 'NONE';

	return {
		get_current_topic : function() {
			return current_topic;
		},
		set_current_topic : function(topic) {
			current_topic = topic;
		}
	}
})