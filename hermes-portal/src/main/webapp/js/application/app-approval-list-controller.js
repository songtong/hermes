application_module.controller('app-approval-list-controller', [ '$scope', 'ApplicationService', function($scope, ApplicationService) {
	$scope.application_statuses = {
		processing : 0,
		rejected : 1,
		success : 2
	};
	$scope.applications = [];
	$scope.application_status = "processing";
	console.log($scope.application_statuses["processing"]);
	ApplicationService.get_applications($scope.application_statuses["processing"]).then(function(result) {
		$scope.applications = result;
		console.log(result);
	})

} ])