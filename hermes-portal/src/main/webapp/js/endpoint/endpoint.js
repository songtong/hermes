angular.module('hermes-endpoint', [ 'ngResource', 'smart-table', 'ui.bootstrap', 'global', 'utils', 'components' ]).controller('endpoint-controller', [ '$scope', '$filter', '$resource', function($scope, filter, resource) {
	var endpoint_resource = resource('/api/endpoints', {}, {
		'update_endpoint' : {
			method : 'POST',
			url : '/api/endpoints/update'
		}
	});

	$scope.groupTypes = [];
	$scope.currentGroupType = '';
	$scope.endpoints = [];
	$scope.currentEndpoints = [];
	$scope.groupTypeToEndpointMap = {};
	$scope.currentEndpoint = null;
	$scope.endpointHosts = [];

	$scope.idleGroupType = 'idle';

	// init
	endpoint_resource.query(function(result) {
		$scope.endpoints = result;
		for (var i = 0; i < $scope.endpoints.length; i++) {
			var groupType = $scope.endpoints[i].group;
			if (groupType != $scope.idleGroupType && !$scope.groupTypes.includes(groupType)) {
				$scope.groupTypes.push(groupType);
			}
			if (!$scope.groupTypeToEndpointMap.hasOwnProperty(groupType)) {
				$scope.groupTypeToEndpointMap[groupType] = [];
			}

			$scope.groupTypeToEndpointMap[groupType].push($scope.endpoints[i]);
			$scope.endpointHosts.push($scope.endpoints[i].host)

		}

		$scope.groupTypes.push($scope.idleGroupType);
		if (!$scope.groupTypeToEndpointMap.hasOwnProperty($scope.idleGroupType)) {
			$scope.groupTypeToEndpointMap[$scope.idleGroupType] = [];
		}

		$scope.switchCurrentGroupType($scope.groupTypes[0]);

		$scope.initGroupTypeTypeahead();
		$scope.initEndpointIpTypeahead();
	})

	$scope.initGroupTypeTypeahead = function() {
		var groupsSuggestions = new Bloodhound({
			datumTokenizer : Bloodhound.tokenizers.whitespace,
			queryTokenizer : Bloodhound.tokenizers.whitespace,
			local : $scope.groupTypes
		});

		$('#groupType').typeahead({
			hint : true,
			highlight : true,
			minLength : 1
		}, {
			name : 'groupsSuggestions',
			source : groupsSuggestions
		});

		$('#groupType').bind('typeahead:select', function(ev, suggestion) {
			$scope.targetGroupType = suggestion;
		});
	}

	$scope.initEndpointIpTypeahead = function() {
		var endpointHostSuggestions = new Bloodhound({
			datumTokenizer : Bloodhound.tokenizers.whitespace,
			queryTokenizer : Bloodhound.tokenizers.whitespace,
			local : $scope.endpointHosts
		});

		$('#searchHost').typeahead({
			hint : true,
			highlight : true,
			minLength : 1
		}, {
			name : 'endpointHostSuggestions',
			source : endpointHostSuggestions
		});

		$('#searchHost').bind('typeahead:select', function(ev, suggestion) {
			$scope.searchHost = suggestion;
		});
	}

	$scope.switchCurrentGroupType = function(groupType) {
		$scope.currentGroupType = groupType;
		$scope.currentEndpoints = $scope.groupTypeToEndpointMap[$scope.currentGroupType];
	}

	$scope.setCurrentEndpoint = function(endpoint) {
		$scope.currentEndpoint = endpoint;
	}

	$scope.searchByip = function(searchHost) {
		for (var i = 0; i < $scope.endpoints.length; i++) {
			if ($scope.endpoints[i].host == searchHost) {
				$scope.currentEndpoint = $scope.endpoints[i];
				$scope.switchCurrentGroupType($scope.endpoints[i].group);
			}
		}
	}

	$scope.offline = function(endpoint) {
		var originGroupType = endpoint.group;
		endpoint.group = $scope.idleGroupType;

		endpoint_resource.update_endpoint(endpoint, function(result) {
			show_op_info.show("更新成功", true);

			var i = 0;
			for (; i < $scope.groupTypeToEndpointMap[originGroupType].length; i++) {
				if ($scope.groupTypeToEndpointMap[originGroupType][i].id == endpoint.id) {
					break;
				}
			}

			$scope.groupTypeToEndpointMap[originGroupType].splice(i, 1);
			$scope.groupTypeToEndpointMap[$scope.idleGroupType].push(endpoint);

			if ($scope.groupTypeToEndpointMap[originGroupType].length == 0) {
				$scope.groupTypes.splice($scope.groupTypes.indexOf(originGroupType), 1);
				delete $scope.groupTypeToEndpointMap[originGroupType]
				$scope.switchCurrentGroupType($scope.idleGroupType);
			}
			
			$scope.currentEndpoint = null;
		}, function(error_result) {
			show_op_info.show("更新失败: " + error_result.data, false);
		});
	}

	$scope.online = function(endpoint, targetGroupType) {
		var originGroupType = endpoint.group;
		if (targetGroupType == "" || targetGroupType == null) {
			show_op_info.show("目标Group不可以为空！", false);
			return;
		}
		endpoint.group = targetGroupType;

		endpoint_resource.update_endpoint(endpoint, function(result) {
			show_op_info.show("更新成功", true);

			var i = 0;
			for (; i < $scope.groupTypeToEndpointMap[originGroupType].length; i++) {
				if ($scope.groupTypeToEndpointMap[originGroupType][i].id == endpoint.id) {
					break;
				}
			}

			$scope.groupTypeToEndpointMap[originGroupType].splice(i, 1);

			if (!$scope.groupTypeToEndpointMap.hasOwnProperty(targetGroupType)) {
				$scope.groupTypeToEndpointMap[targetGroupType] = [];
				$scope.groupTypes.pop();
				$scope.groupTypes.push(targetGroupType, $scope.idleGroupType);
			}
			$scope.groupTypeToEndpointMap[targetGroupType].push(endpoint);
			
			$scope.currentEndpoint = null;

		}, function(error_result) {
			show_op_info.show("更新失败: " + error_result.data, false);
		});

	}

} ]);
