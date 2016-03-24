/*
 * UI components.
 * @uknow.
 */
angular.directive('progressBar', function() {
	return {
		restrict: 'A',
		template: '<div class="progress">' 
			+ '<div class="progress-bar progress-bar-striped active" role="progressbar" aria-valuenow="progressBarValue" aria-valuemin="0" aria-valuemax="100" style="min-width: 2em; width: 0%;"></div>'
			+ '</div>',
		link: function($scope, $element, attrs) {
			//return $scope;
		}
	};
});