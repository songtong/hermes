dashtopic.controller('dash-topic-controller', function($scope, $http, $resource, $sce, $routeParams, DashboardTopicService) {
	var latest_msgs_resource = $resource('/api/monitor/topics/:topic/latest');
	var topic_delay_resource = $resource('/api/monitor/detail/topics/:topic/delay', {}, {
		query : {
			method : "GET",
			isArray : false
		}
	});

	if ($scope.topic_briefs == undefined) {
		console.log('Topic brief is undefined, refresh it.')
		$http.get('/api/monitor/brief/topics').success(function(data, status, headers, config) {
			$scope.topic_briefs = data;
		}).error(function(data, status, headers, config) {
			console.log(data);
		});
	}

	var route_topic = $routeParams['topic'];
	$scope.current_topic = route_topic == "_default" ? $scope.topic_briefs != undefined ? $scope.topic_briefs[0].topic : 'leo_test' : route_topic;
	$scope.current_consumer = $routeParams['consumer'];
	DashboardTopicService.set_current_topic($scope.current_topic);

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
	$scope.refresh_topic_latest = function() {
		latest_msgs_resource.query({
			'topic' : $scope.current_topic
		}, function(data) {
			$scope.topic_latest = data;
		});
	}
	$scope.topic_latest = $scope.refresh_topic_latest();

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
	
	$scope.show_payload = function(payload){
		$scope.current_payload = payload;
		$("#payload-view").modal('show');
	}

	// ************** consumer delays **************** //
	$scope.refresh_topic_delay = function() {
		topic_delay_resource.query({
			'topic' : $scope.current_topic
		}, function(data) {
			$scope.topic_delay=[];
			for(var consumer_name in data.details){
				$scope.topic_delay=$scope.topic_delay.concat(data.details[consumer_name]);
			}
			console.log($scope.topic_delay);
		});
	}
	$scope.topic_delay = $scope.refresh_topic_delay();
	$scope.display_topic_delay = [].concat($scope.topic_delay);
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