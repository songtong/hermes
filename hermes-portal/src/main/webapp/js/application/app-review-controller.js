application_module.controller('app-review-controller', [ '$scope', '$routeParams', 'ApplicationService', '$filter', function($scope, $routeParams, ApplicationService, $filter) {
	$scope.productLines = ApplicationService.get_productLines();
	$scope.storageTypes = [ 'mysql', 'kafka' ];
	$scope.codecTypes = [ 'json', 'avro' ];
	$scope.languageTypes = [ 'java', '.net' ];
	$scope.needRetryList = [{
		value : true,
		text : "是"
	}, {
		value : false,
		text : "否"
	}];
	
	function decodeComment() {
		if ($scope.application.comment) {
			try {
				$scope.application.comment = JSON.parse($scope.application.comment);
			} catch (e) {
				// ignore for the case comment is old-style.
			}
			
			// Compatible with the old-style comment.
			if (!($scope.application.comment instanceof Array)) {
				$scope.application.comment = [{
					createdTime: $filter('date')(new Date($scope.application.lastModifiedTime), 'yyyy-MM-dd HH:mm:ss'),
					author: $scope.application.approver,
					comment: $scope.application.comment
				}];
			}
		} else {
			$scope.application.comment = [];
		}
	}
	
	$scope.application = ApplicationService.get_application($routeParams['id']).then(function(result) {
		$scope.application = result;
		decodeComment();
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
			decodeComment();
			//$scope.application.needRetry = $scope.application.needRetry.toString();
		}, function(result) {
			show_op_info.show("Update application failed, please try later.", false);
		})
	}
}]);