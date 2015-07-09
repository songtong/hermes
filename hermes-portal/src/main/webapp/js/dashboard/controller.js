/**
 * Created by ske on 2015/6/1.
 */

dashtopic.controller("dash-topic-controller", function($scope, $http, $resource, $compile, DashtopicService) {
	$scope.topic_briefs = [];
	$scope.main_board_content = {};
	$scope.current_topic = "";
	$scope.topic_delay = [];

	DashtopicService.update_topic_briefs();
	// DashtopicService.update_topic_delays(DashtopicService.get_current_topic());

	$scope.$watch(DashtopicService.get_topic_briefs, function() {
		$scope.topic_briefs = DashtopicService.get_topic_briefs();
	});

	$scope.$watch(DashtopicService.get_current_topic, function() {
		$scope.current_topic = DashtopicService.get_current_topic();
	});

	$scope.$watch(DashtopicService.get_main_board_content, function() {
		$scope.main_board_content = DashtopicService.get_main_board_content($scope.current_topic);
		$("#main_board").html($scope.main_board_content);
		$compile($("#main_board"))($scope);
	});

	$scope.nav_select = function nav_select(topic_brief) {
		DashtopicService.update_main_board_content(topic_brief.topic);
		$("#main_board").html($scope.main_board_content);
		$compile($("#main_board"))($scope);
	};
});