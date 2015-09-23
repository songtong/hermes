function recombineDelays(data) {
	temp_consume_delays_detail = [];
	var index = 0;
	for (var i = 0; i < data.length; i++) {
		topic_name = data[i].topic;
		temp_delay_details = data[i].details;
		for ( var consumer_name in temp_delay_details) {
			var tempDelay=0;
			for (var k = 0; k <  temp_delay_details[consumer_name].length; k++) {
				tempDelay +=  temp_delay_details[consumer_name][k].delay;
			}
			temp_consume_delays_detail.push({
				topic : topic_name,
				consumer : consumer_name,
				delay : tempDelay,
				details : temp_delay_details[consumer_name]
			});
		}
	}
	return temp_consume_delays_detail;
}

var homeapp = angular.module("dashboard", [ 'ngResource', 'ui.bootstrap',
		'smart-table' ]);

homeapp.filter('short', function() {
	return function(input, length) {
		input = input || '';
		length = length || 30;
		input = input.replace(/\\"/g, '"');
		if (input.length <= length) {
			return input;
		}
		out = input.substring(0, length / 2) + " ... "
				+ input.substring(input.length - length / 2);
		return out;
	}
});

homeapp.filter('produce_format', function() {
	return function(input) {
		input = input || 'NO_MSG';
		return input == '1970-01-01 08:00:00' ? 'NO_MSG' : input
	}
});

homeapp.controller("hermes-dashboard-controller", function($scope, $http,
		$resource, $compile, $sce) {
	$scope.consume_delays_detail = [];
	$scope.oudate_topics = [];
	$scope.broker_received_qps = [];
	$scope.broker_delivered_qps = [];

	$scope.display_consume_delays_deail = []
			.concat($scope.consume_delays_detail);
	$scope.display_outdate_topics = [].concat($scope.outdate_topics);
	$scope.display_broker_received_qps = [].concat($scope.broker_received_qps);
	$scope.display_broker_delivered_qps = []
			.concat($scope.broker_delivered_qps);

	$scope.delay_table_is_loading = true;
	$scope.outdate_topics_table_is_loading = true;
	$scope.broker_received_table_is_loading = true;
	$scope.broker_delivered_table_is_loading = true;

	var dashboard_resource = $resource("", {}, {
		get_consume_delays : {
			method : 'GET',
			isArray : true,
			url : '/api/monitor/top/delays'
		},
		get_outdate_topics : {
			method : 'GET',
			isArray : true,
			url : '/api/monitor/top/outdate-topics'
		},
		get_brokers_received_qps : {
			method : 'GET',
			isArray : true,
			url : '/api/monitor/top/broker/qps/received'
		},
		get_brokers_delivered_qps : {
			method : 'GET',
			isArray : true,
			url : '/api/monitor/top/broker/qps/delivered'
		},
		get_broker_received_qps : {
			method : 'GET',
			isArray : false,
			url : '/api/monitor/top/broker/qps/received/:brokerIp'
		},
		get_broker_delivered_qps : {
			method : 'GET',
			isArray : false,
			url : '/api/monitor/top/broker/qps/delivered/:brokerIp'
		}
	});

	function update_datas() {
		dashboard_resource.get_consume_delays({}, function(data) {
			$scope.consume_delays_detail = recombineDelays(data);
			$scope.delay_table_is_loading = false;
		});

		dashboard_resource.get_outdate_topics({}, function(data) {
			$scope.outdate_topics = data;
			$scope.outdate_topics_table_is_loading = false;
		});

		dashboard_resource.get_brokers_received_qps({}, function(data) {
			$scope.broker_received_qps = data;
			$scope.broker_received_table_is_loading = false;
		});

		dashboard_resource.get_brokers_delivered_qps({}, function(data) {
			$scope.broker_delivered_qps = data;
			$scope.broker_delivered_table_is_loading = false;
		});
	}

	update_datas();

	$scope.get_delay_detail = function(delay) {
		$scope.current_delay_details = delay.details;
		return "/jsp/console/home/delay-detail.html";
	}

	$scope.get_broker_received_detail = function(broker) {
		dashboard_resource.get_broker_received_qps({
			'brokerIp' : broker
		}, function(data) {
			$scope.current_broker_received_details = data;
		})
	}

	$scope.get_broker_delivered_detail = function(broker) {
		console.log(broker);
		dashboard_resource.get_broker_delivered_qps({
			'brokerIp' : broker
		}, function(data) {
			console.log(data);
			$scope.current_broker_delivered_details = data;
		})
	}

	$scope.normalize_delay = function(delay) {
		if (delay >= 86400000) {
			return parseInt(delay / 86400000) + "天"
					+ $scope.normalize_delay(delay % 86400000);
		}
		if (delay >= 3600000) {
			return parseInt(delay / 3600000) + "小时"
					+ $scope.normalize_delay(delay % 3600000);
		}
		if (delay >= 60000) {
			return parseInt(delay / 60000) + "分钟"
					+ $scope.normalize_delay(delay % 60000);
		}
		return parseInt(delay / 1000) + "秒";
	};

	$scope.get_delay_to_now = function(before) {
		if (before == 0) {
			return "NO_MSG_FOUND";
		}
		return $scope.normalize_delay(parseInt(new Date().getTime()) - before);
	}

	$scope.getters = {
		outdate_delay_to_now : function(value) {
			return $scope.get_delay_to_now(value.value);
		}
	}
});