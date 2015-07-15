angular.module("dashboard", [ 'ngResource', 'ui.bootstrap', 'smart-table' ]).controller("hermes-dashboard-controller",
		function($scope, $http, $resource, $compile, $sce) {
			$scope.consume_delays = [];
			$scope.consume_delays_detail = {};
			$scope.oudate_topics = [];
			$scope.broker_received_qps = [];
			$scope.broker_delivered_qps = [];

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

			dashboard_resource.get_consume_delays({}, function(data) {
				$scope.consume_delays = data;
				for (var i = 0; i < $scope.consume_delays.length; i++) {
					$scope.consume_delays_detail[$scope.consume_delays[i].topic] = $scope.consume_delays[i].details;
				}
			});

			function update_datas() {
				dashboard_resource.get_outdate_topics({}, function(data) {
					$scope.outdate_topics = data;
				});

				dashboard_resource.get_brokers_received_qps({}, function(data) {
					$scope.broker_received_qps = data;
				});

				dashboard_resource.get_brokers_delivered_qps({}, function(data) {
					$scope.broker_delivered_qps = data;
				});
			}

			update_datas();

			$scope.get_delay_detail = function(topic) {
				$scope.current_delay_details = $scope.consume_delays_detail[topic];
				return "delay_popover_template";
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
					return parseInt(delay / 86400000) + "天" + $scope.normalize_delay(delay % 86400000);
				}
				if (delay >= 3600000) {
					return parseInt(delay / 3600000) + "小时" + $scope.normalize_delay(delay % 3600000);
				}
				if (delay >= 60000) {
					return parseInt(delay / 60000) + "分钟" + $scope.normalize_delay(delay % 60000);
				}
				return parseInt(delay / 1000) + "秒";
			};

			$scope.get_delay_to_now = function(before) {
				if (before == 0) {
					return "NO_MSG_FOUND";
				}
				return $scope.normalize_delay(parseInt(new Date().getTime()) - before);
			}
		});