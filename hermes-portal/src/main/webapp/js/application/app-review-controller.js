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
	} ]
	$scope.application = ApplicationService.get_application($routeParams['id']).then(function(result) {
		$scope.application = result;
		$scope.application.needRetry = $scope.application.needRetry.toString();
		updatePageType(result.type);
	})
	$scope.update_application = function(data) {
		console.log(data);
		ApplicationService.update_application($scope.application, $scope.application.type).then(function(result) {
			show_op_info.show("Update application success!", true);
			$scope.application = result;
			$scope.application.needRetry = $scope.application.needRetry.toString();
		}, function(result) {
			show_op_info.show("Update application failed, please try later.", false);
		})
	}

	function updatePageType(typeCode) {
		switch (typeCode) {
		case 0:
			console.log("Topic创建");
			$scope.currentPageType = "Topic创建";
			break;
		case 1:
			$scope.currentPageType = "Consumer创建";
			break;
		case 2:
			$scope.currentPageType = "Topic修改";
			break;
		case 3:
			$scope.currentPageType = "Consumer修改";
			break;
		default:
			console.log("default");
		}
	}

} ])