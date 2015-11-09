application_module.controller('app-approval-detail-controller', [ '$scope', '$routeParams', '$resource', 'ApplicationService', function($scope, $routeParams, $resource, ApplicationService) {
	ApplicationService.get_generated_application($routeParams['id']).then(function(result) {
		console.log(result);
		$scope.application = result["key"];
		$scope.view = result["value"];

	})
	var topic_resourse = $resource("/api/topics/:name", {}, {});
	var consumer_resourse = $resource("api/consuemrs", {}, {});
	var meta_resource = $resource('/api/meta/storages', {}, {
		'get_storage' : {
			method : 'GET',
			isArray : true,
		}
	});
} ])