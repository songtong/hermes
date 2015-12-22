monitor.controller('dash-monitor-event-controller', function($scope, $http, $resource) {
	$scope.topic_relevant_events = [ "PRODUCE_ACKED_TRIED_RATIO_ERROR", "CONSUME_LARGE_BACKLOG", "CONSUME_LARGE_DELAY", "PARTITION_MODIFICATION", "PRODUCE_LARGE_FAILURE_COUNT", "PRODUCE_LARGE_LATENCY", "TOPIC_LARGE_DEAD_LETTER" ];
	$scope.monitor_event_displayed = [];
	$scope.find_monitor_events = function find_monitor_event(tableState) {
		$scope.isLoading = true;
		var pagination = tableState.pagination;
		var start = pagination.start || 0;
		var number = pagination.number || 20;

		$resource('/api/monitor/event/detail').get({
			'pageCount' : number,
			'pageNum' : start
		}, function(result) {
			$scope.monitor_event_displayed = result.datas;
			tableState.pagination.numberOfPages = result.totalPage;
			$scope.isLoading = false;
		});
	};
	$scope.show_message = function(payload) {
		$scope.current_message = payload.replace(/\\n/g, '<br>');
		$("#message-view").modal('show');
	}

	$scope.jump = function(eventType, topicName) {
		return ($scope.topic_relevant_events.indexOf(eventType) != -1)
	}

});