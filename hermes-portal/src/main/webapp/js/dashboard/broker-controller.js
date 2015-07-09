angular
		.module("dash-broker", [ 'ngResource', 'ui.bootstrap' ])
		.controller(
				"dash-broker-controller",
				function($scope, $http, $resource, $compile, $sce) {
					$scope.brokers = [];
					$scope.current_broker = "";
					$scope.main_board_content = "";

					var monitor_resource = $resource("/api/monitor/", {}, {
						get_brokers : {
							method : "GET",
							isArray : true,
							url : "/api/monitor/brief/brokers"
						},
					});

					monitor_resource.get_brokers({}, function(data) {
						$scope.brokers = data;
					});

					function update_main_board(url) {
						$http.get(url).success(function(data, status, headers, config) {
							$scope.main_board_content = data;
							$("#main_board").html($scope.main_board_content);
							$compile($("#main_board"))($scope);
						}).error(function(data, status, headers, config) {
							console.log(data);
						});
					}

					$scope.$watch($scope.main_board_content, function() {
						$("#main_board").html($scope.main_board_content);
					});

					update_main_board('/console/dashboard?op=broker-detail-home');

					$scope.nav_select = function(broker) {
						if (broker == '_broker_main') {
							var url = '/console/dashboard?op=broker-detail-home';
							update_main_board(url);
						} else {
							var url = '/console/dashboard?op=broker-detail&broker=' + broker;
							$scope.current_broker = broker;
							update_main_board(url);
						}
					};

					$scope.get_received_top_kibana = function(kibanaUrl) {
						var url = kibanaUrl
								+ "/#/visualize/edit/Broker-Top?embed&_g=(refreshInterval:(display:'10%20seconds',pause:!f,section:1,value:10000),time:(from:now-1m,mode:relative,to:now))&_a=(filters:!(),linked:!f,query:(query_string:(analyze_wildcard:!t,query:'eventType:Message.Received')),vis:(aggs:!((id:'1',params:(),schema:metric,type:count),(id:'2',params:(field:datas.brokerIp.raw,order:desc,orderBy:'1',size:100),schema:bucket,type:terms)),listeners:(),params:(perPage:10,showMeticsAtAllLevels:!f,showPartialRows:!f),type:table))"
						return $sce.trustAsResourceUrl(url);
					};

					$scope.get_delivered_top_kibana = function(kibanaUrl) {
						var url = kibanaUrl
								+ "/#/visualize/edit/Broker-Delivered-Top?embed&_g=(refreshInterval:(display:'10%20seconds',pause:!f,section:1,value:10000),time:(from:now-1m,mode:relative,to:now))&_a=(filters:!(),linked:!f,query:(query_string:(analyze_wildcard:!t,query:'eventType:Message.Delivered')),vis:(aggs:!((id:'1',params:(),schema:metric,type:count),(id:'2',params:(field:datas.brokerIp.raw,order:desc,orderBy:'1',size:100),schema:bucket,type:terms)),listeners:(),params:(perPage:10,showMeticsAtAllLevels:!f,showPartialRows:!f),type:table))"
						return $sce.trustAsResourceUrl(url);
					};

					$scope.get_br_received_top_kibana = function(kibanaUrl) {
						var url = kibanaUrl
								+ "/#/visualize/edit/Broker-Topic-Received-Top?embed&_g=(refreshInterval:(display:'10%20seconds',pause:!f,section:1,value:10000),time:(from:now-1m,mode:relative,to:now))&_a=(filters:!(),linked:!f,query:(query_string:(analyze_wildcard:!t,query:'datas.brokerIp:"
								+ $scope.current_broker
								+ "%20AND%20eventType:Message.Received')),vis:(aggs:!((id:'1',params:(),schema:metric,type:count),(id:'2',params:(field:datas.topic.raw,order:desc,orderBy:'1',size:50),schema:bucket,type:terms),(id:'3',params:(field:datas.producerIp.raw,order:desc,orderBy:'1',size:50),schema:bucket,type:terms)),listeners:(),params:(perPage:10,showMeticsAtAllLevels:!f,showPartialRows:!f),type:table))";
						return $sce.trustAsResourceUrl(url);
					};

					$scope.get_br_received_bottom_kibana = function(kibanaUrl) {
						var url = kibanaUrl
								+ "/#/visualize/edit/Broker-Topic-Received-Bottom?embed&_g=(refreshInterval:(display:'10%20seconds',pause:!f,section:1,value:10000),time:(from:now-1m,mode:relative,to:now))&_a=(filters:!(),linked:!f,query:(query_string:(analyze_wildcard:!t,query:'datas.brokerIp:"
								+ $scope.current_broker
								+ "%20AND%20eventType:Message.Received')),vis:(aggs:!((id:'1',params:(),schema:metric,type:count),(id:'2',params:(field:datas.topic.raw,order:asc,orderBy:'1',size:50),schema:bucket,type:terms),(id:'3',params:(field:datas.producerIp.raw,order:asc,orderBy:'1',size:50),schema:bucket,type:terms)),listeners:(),params:(perPage:10,showMeticsAtAllLevels:!f,showPartialRows:!f),type:table))";
						return $sce.trustAsResourceUrl(url);
					};

					$scope.get_br_received_qps_kibana = function(kibanaUrl) {
						var url = kibanaUrl
								+ "/#/visualize/edit/BR-QPS?embed&_g=(refreshInterval:(display:'5%20seconds',pause:!f,section:1,value:5000),time:(from:now-1m,mode:relative,to:now))&_a=(filters:!(),linked:!f,query:(query_string:(analyze_wildcard:!t,query:'eventType:Message.Received%20AND%20datas.brokerIp:"
								+ $scope.current_broker
								+ "')),vis:(aggs:!((id:'1',params:(),schema:metric,type:count)),listeners:(),params:(fontSize:'36'),type:metric))";
						return $sce.trustAsResourceUrl(url);
					};

					$scope.get_br_deliver_qps_kibana = function(kibanaUrl) {
						var url = kibanaUrl
								+ "/#/visualize/edit/BD-QPS?embed&_g=(refreshInterval:(display:'5%20seconds',pause:!f,section:1,value:5000),time:(from:now-1m,mode:relative,to:now))&_a=(filters:!(),linked:!f,query:(query_string:(analyze_wildcard:!t,query:'eventType:Message.Delivered%20AND%20datas.brokerIp:"
								+ $scope.current_broker
								+ "')),vis:(aggs:!((id:'1',params:(),schema:metric,type:count)),listeners:(),params:(fontSize:'36'),type:metric))";
						return $sce.trustAsResourceUrl(url);
					};

				});