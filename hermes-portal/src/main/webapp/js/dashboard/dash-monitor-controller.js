monitor.controller('dash-monitor-event-controller', function($scope, $http, $resource) {
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
});