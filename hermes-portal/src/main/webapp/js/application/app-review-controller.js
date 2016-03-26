application_module.controller('app-review-controller', [ '$scope', '$routeParams', 'ApplicationService', function($scope, $routeParams, ApplicationService) {
	$scope.productLines = ApplicationService.get_productLines();
	$scope.storageTypes = [ 'mysql', 'kafka' ];
	$scope.codecTypes = [ 'json', 'avro' ];
	$scope.languageTypes = [ 'java', '.net' ];
	$scope.needRetryList = [ {
		value : true,
		text : "是"
	}, {
		value : false,
		text : "否"
	} ];
	
	$scope.application = ApplicationService.get_application($routeParams['id']).then(function(result) {
		$scope.application = result;
		if ($scope.application.needRetry) {
			$scope.application.needRetry = $scope.application.needRetry.toString();
		}
	})
	
	$scope.updateApplication = function(status) {
		if ($scope.application.status == 3 && !$scope.application.onlineEnv) {
			$scope.$broadcast('alert-error', 'alert', '必须选择一个上线环境！');
			return;
		}

		$scope.application.status = status;
		ApplicationService.update_application($scope.application).then(function(result) {
			$scope.application = result;
			//$scope.application.needRetry = $scope.application.needRetry.toString();
		}, function(result) {
			show_op_info.show("Update application failed, please try later.", false);
		})
	}
}]);