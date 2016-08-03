angular.module('hermes-tracer', [ 'ngResource', 'ui.bootstrap', 'global', 'utils', 'components' ]).controller('tracer-controller', [ '$scope', '$resource', function($scope, $resource) {
	var tracerResource = $resource("/api/tracer", {}, {})
	var topicResource = $resource('/api/topics/names', {}, {})
	$scope.msgDate = new Date();

	$scope.mlcs = [];
	$scope.unUsedEvents = [];
	$scope.hintWord = "";
	$scope.topicNames = [];
	$scope.topicName = "";
	$scope.refKey = "";

	topicResource.query(function(result) {
		result = new Bloodhound({
			datumTokenizer : Bloodhound.tokenizers.whitespace,
			queryTokenizer : Bloodhound.tokenizers.whitespace,
			local : result
		});

		$('#topicNames').typeahead({
			hint : true,
			highlight : true,
			minLength : 1
		}, {
			name : 'result',
			source : result
		});

		$('#topicNames').bind('typeahead:select', function(ev, suggestion) {
			$scope.topicName = suggestion;
		});
	})

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