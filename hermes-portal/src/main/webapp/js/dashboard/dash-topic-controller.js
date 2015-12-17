dashtopic.controller('dash-topic-controller', function($scope, $http, $resource, $sce, $routeParams, DashboardTopicService) {
	var route_topic = $routeParams['topic'];
	var route_consumer = $routeParams['consumer'];

	var topic_delay_resource = $resource('/api/dashboard/detail/topics/:topic/delay', {}, {
		query : {
			method : "GET",
			isArray : false
		}
	});
	$scope.current_topic = route_topic == "_default" ? $scope.topic_briefs != undefined ? $scope.topic_briefs[0].topic : 'leo_test' : route_topic;
	$scope.current_consumer = $routeParams['consumer'];
	DashboardTopicService.get_topic_briefs().then(function(result) {
		$scope.topic_briefs = result;
		$scope.topic = find($scope.current_topic, $scope.topic_briefs);
		if ($scope.topic.storageType == 'kafka') {
			$scope.trifecta_urls = DashboardTopicService.get_trifecta_urls();
		} else if ($scope.topic.storageType = 'mysql') {
			// ************** kibana urls **************** //
			$scope.k_topic_produce_history = k_topic_produce_history($scope.current_topic, $sce);
			$scope.k_consume_history = k_consumer_consume_history($scope.current_topic, $scope.current_consumer, $sce);
			$scope.k_consume_process_history = k_consumer_process_history($scope.current_topic, $scope.current_consumer, $sce);
			$scope.k_top_producer_current = k_top_producer_current($scope.current_topic, $sce);
			$scope.k_bottom_producer_current = k_bottom_producer_current($scope.current_topic, $sce);
			$scope.k_top_consumer_current = k_top_consumer_current($scope.current_topic, $sce);
			$scope.k_bottom_consumer_current = k_bottom_consumer_current($scope.current_topic, $sce);
			$scope.k_bottom_process_current = k_bottom_process_current($scope.current_topic, $sce);
			$scope.k_max_did_current = k_max_did_current($scope.current_topic, $sce);
			$scope.k_max_aid_current = k_max_aid_current($scope.current_topic, $sce);

			// ************** latest messages **************** //
			DashboardTopicService.get_topic_latest_msgs($scope.current_topic).then(function(result) {
				$scope.topic_latest = result;
			});
			
			// ************** consumer delays **************** //
			$scope.consumers = DashboardTopicService.get_consumers_for_topic($scope.current_topic).then(function(result) {
				$scope.consumers = result;
				$scope.display_filtered_consumers = $scope.consumers;
			});

			if ($scope.current_consumer != null) {
				$scope.consumer_delay = DashboardTopicService.get_consumer_delay_for_topic($scope.current_topic, $scope.current_consumer).then(function(result) {
					$scope.consumer_delay = result;
					console.log(result);
				})
			}

			$scope.display_topic_delay = [].concat($scope.topic_delay);
		}
	})

	DashboardTopicService.set_current_topic($scope.current_topic);

	$scope.show_tree = function(ref_key, json_str, replace) {
		$scope.current_refkey = ref_key;
		if (replace) {
			json_str = json_str.replace(/\\"/g, '"');
		}
		if (starts_with(json_str, '"{') && ends_with(json_str, '}"')) {
			json_str = json_str.substring(1, json_str.length - 1);
		}
		var obj = JSON.parse(json_str);
		$scope.current_attr_json = obj;
		$("#data-tree").treeview({
			data : format_tree(obj),
			levels : 1
		});
		$("#attr-view").modal('show');
	};

	$scope.show_payload = function(payload) {
		payload = payload.replace(/\\"/g, '"');
		if (starts_with(payload, '"{') && ends_with(payload, '}"')) {
			payload = payload.substring(1, payload.length - 1);
		}
		var obj = JSON.parse(payload);
		$scope.current_payload = obj;
		$("#payload-view").modal('show');
	}

});

function format_tree(obj) {
	var data = [];
	if (obj instanceof Object) {
		for ( var key in obj) {
			var node = {};
			var value = obj[key];
			node.text = key;
			if (value instanceof Object) {
				node.nodes = format_tree(value);
			} else {
				node.nodes = [ {
					text : value
				} ];
			}
			data.push(node);
		}
	} else {
		data.push({
			text : obj
		});
	}
	return data;
}

function find(key, list) {
	for (var i = 0; i < list.length; i++) {
		if (list[i].topic == key) {
			return list[i];
		}
	}
	return null;
}