angular
		.module("dash-client", [ 'ngResource', 'ui.bootstrap' ])
		.controller(
				"dash-client-controller",
				function($scope, $http, $resource, $compile, $sce) {
					$scope.current_ip = current_client;
					$scope.declare_topics = {
						"produceTopics" : [],
						"consumeTopics" : []
					};

					var monitor_resource = $resource("/api/dashboard/", {}, {
					});

					$scope.$watch($scope.declare_topics, function() {
					});

					$scope.get_client_produced_kibana = function(kibanaUrl, producedTopic) {
						var url = kibanaUrl
								+ "/#/visualize/edit/IP-Produced?embed&_g=(refreshInterval:(display:'30%20seconds',pause:!f,section:1,value:30000),time:(from:now-1h,mode:quick,to:now))&_a=(filters:!(),linked:!f,query:(query_string:(analyze_wildcard:!t,query:'eventType:Message.Received%20AND%20datas.producerIp:"
								+ $scope.current_ip
								+ "%20AND%20datas.topic:"
								+ producedTopic
								+ "')),vis:(aggs:!((id:'1',params:(),schema:metric,type:count),(id:'2',params:(customInterval:'2h',extended_bounds:(),field:'@timestamp',interval:auto,min_doc_count:1),schema:segment,type:date_histogram)),listeners:(),params:(addLegend:!f,addTimeMarker:!f,addTooltip:!t,defaultYExtents:!f,mode:stacked,scale:linear,setYExtents:!f,shareYAxis:!t,times:!(),yAxis:()),type:histogram))";
						return $sce.trustAsResourceUrl(url);
					}

					$scope.get_client_consumed_kibana = function(kibanaUrl, group, consumed_topic) {
						var url = kibanaUrl
								+ "/#/visualize/edit/IP-Consumed?embed&_g=(refreshInterval:(display:'30%20seconds',pause:!f,section:1,value:30000),time:(from:now-1h,mode:quick,to:now))&_a=(filters:!(),linked:!f,query:(query_string:(analyze_wildcard:!t,query:'eventType:Message.Acked%20AND%20datas.consumerIp:"
								+ $scope.current_ip
								+ "%20AND%20datas.topic:"
								+ consumed_topic
								+ "%20AND%20datas.groupId:"
								+ group
								+ "')),vis:(aggs:!((id:'1',params:(),schema:metric,type:count),(id:'2',params:(customInterval:'2h',extended_bounds:(),field:'@timestamp',interval:auto,min_doc_count:1),schema:segment,type:date_histogram)),listeners:(),params:(addLegend:!f,addTimeMarker:!f,addTooltip:!t,defaultYExtents:!f,mode:stacked,scale:linear,setYExtents:!f,shareYAxis:!t,times:!(),yAxis:()),type:histogram))";
						return $sce.trustAsResourceUrl(url);
					}

					$scope.selected = undefined;
				});