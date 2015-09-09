function format(source) {
	var value = "";
	for (var idx = 0; idx < source.length; idx++) {
		value = value + source[idx] + "\n";
	}
	return value;
}

angular.module("exchange-app", [ "ngResource" ]).controller("exchange-controller", [ "$scope", "$resource", function($scope, $resource) {
	$scope.current_content = [];
	$resource("/api/cmessage/exchange").query(function(data) {
		$scope.current_content = format(data);
	});
} ]);