application_module.controller('test', ['$scope', function($scope){
	$scope.type = 'striped';
	$scope.percent = 20;

	$scope.test = function() {
		console.log('eee')
		$scope.$broadcast('progress-random');
	}
	
}]);