dashtopic.controller('dash-topic-nav-controller', function($scope, $http, DashboardTopicService) {
	$http.get('/api/monitor/brief/topics').success(function(data, status, headers, config) {
		$scope.topic_briefs = data;
	}).error(function(data, status, headers, config) {
		console.log(data);
	});
	$scope.$watch(DashboardTopicService.get_current_topic, function() {
		$scope.nav_current_topic = DashboardTopicService.get_current_topic();
	});
});