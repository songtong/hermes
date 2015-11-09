application_module.controller('app-review-controller', [ '$scope', '$routeParams', 'ApplicationService', function($scope, $routeParams, ApplicationService) {
	$scope.productLines = [ 'hotel', 'flight', 'fx' ];
	$scope.storageTypes = [ 'mysql', 'kafka' ];
	$scope.codecTypes = [ 'json', 'avro' ];
	$scope.languageTypes = [ 'java', '.net' ];
	
	$scope.application = ApplicationService.get_application($routeParams['id']).then(function(result) {
		$scope.application = result;
		updatePageType(result.type);
		console.log(result);
	})
	$scope.update_application = function(application) {
		ApplicationService.update_application(application).then(function(reuslt) {
			show_op_info("Update application success!", true);
			$scope.application = ApplicationService.get_application(id).then(function(result) {
				$scope.application = result;
			});
		}, function(result) {
			show_op_info("Update application failed, please try later.", false);
		})
	}

	//$scope.$watch($scope.application.type, updateType);

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