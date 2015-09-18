dashtopic.controller('dash-topic-nav-controller', function($scope, $http, DashboardTopicService) {
	$http.get('/api/monitor/brief/topics').success(function(data, status, headers, config) {
		$scope.topic_briefs = data;
		$scope.filtered_topic_briefs = $scope.topic_briefs;
	}).error(function(data, status, headers, config) {
		console.log(data);
	});
	$scope.$watch(DashboardTopicService.get_current_topic, function() {
		$scope.nav_current_topic = DashboardTopicService.get_current_topic();
	});

	$scope.$watch("filter_key", function() {
		var new_filtered = [];
		if ($scope.topic_briefs != undefined && $scope.filter_key != undefined && $scope.filter_key.length > 0) {
			var key = $scope.filter_key.toLowerCase();
			for (var i = 0; i < $scope.topic_briefs.length; i++) {
				if ($scope.topic_briefs[i].topic.toLowerCase().search(key) >= 0) {
					new_filtered.push($scope.topic_briefs[i]);
				}
			}
			$scope.filtered_topic_briefs = new_filtered;
		} else if ($scope.filter_key != undefined && $scope.filter_key.length == 0) {
			$scope.filtered_topic_briefs = $scope.topic_briefs;
		}
	});
});