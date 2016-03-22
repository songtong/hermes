angular.module("config-app", [ "ngResource" ]).controller("config-controller", [ "$scope", "$resource", function($scope, $resource) {
	$scope.current_content = "";
	$resource("/api/cmessage/config",{},
		{
			get_config : {
				method : 'GET',
				isArray : false
			}
		}
	).get_config(function(data) {
		$scope.current_content = data;
	});
} ]);