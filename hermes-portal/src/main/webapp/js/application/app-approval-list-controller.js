application_module.controller('app-approval-list-controller', [ '$scope', 'ApplicationService', function($scope, ApplicationService) {
	$scope.applications = [];
	$scope.status = '0';
	$scope.initialized = false;
	
	var __state = null;
	$scope.paginate = function(state) {
		if ($scope.initialized) {
			$scope.$emit('progress-random', 'progress', 3000);
		}
		
		__state = state;
		var pagination = state.pagination;
		var start = pagination.start || 0;
		var size = pagination.number || 10;
		
		ApplicationService.get_applications($scope.status, isAdmin? null: ssoMail, start, size).then(function(result) {
			$scope.applications = result['data'];
		    pagination.numberOfPages = Math.ceil(result.count / size);
			$scope.$emit('initialized');
			if ($scope.initialized) {
				$scope.$emit('progress-done', 'progress');
			}
			$scope.initialized = true;
		});
	};
	
	$scope.selectPage = function(page) {
		if (__state) {
			__state.pagination.start = 0;
			$scope.paginate(__state);
		}
	};
} ])