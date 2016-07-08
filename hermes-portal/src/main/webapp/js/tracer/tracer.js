angular.module('hermes-tracer', [ 'ngResource', 'ui.bootstrap', 'global', 'utils', 'components' ]).controller('tracer-controller', [ '$scope', '$resource', function($scope, $resource) {
	var tracerResource = $resource("/api/tracer", {}, {})
	$scope.msgDate = new Date();

	$scope.mlcs = [];
	$scope.unUsedEvents = [];
	$scope.hintWord = "";

	console.log("done");

	$scope.trace = function trace(topicName, refKey, msgDate) {
		if (msgDate.getTime() > new Date()) {
			$scope.$broadcast('alert-error', 'alert', '无效的时间！');
			console.log("error");
			return;
		}
		$scope.hintWord = "查询中..."
		tracerResource.get({
			'topicName' : topicName,
			'refKey' : refKey,
			'date' : msgDate.getTime()
		}, function(result) {
			console.log(result);
			$scope.mlcs = result.key;
			$scope.unUsedEvents = result.value;
			if ($scope.mlcs.length == 0) {
				$scope.hintWord = "未找到结果！"
			} else {
				$scope.hintWord = "";
			}

		}, function(result) {
			$scope.$broadcast('alert-error', 'alert', result.data);
			$scope.hintWord = "";
		});

	};
} ]);

$(function() {

})