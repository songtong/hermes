angular.module('zkApp', [ 'ngResource', 'smart-table', 'xeditable', 'ui.bootstrap' ]).run(function(editableOptions) {
	editableOptions.theme = 'bs3';
}).controller('zkController', [ '$scope', '$resource', '$window', function($scope, $resource, $window) {
	var zookeeperResource = $resource("/api/zookeepers", {}, {
		deleteZookeeperEnsemble : {
			method : 'DELETE',
			url : '/api/zookeepers/:zookeeperEnsembleId',
			params : {
				zookeeperEnsembleId : '@zookeeperEnsembleId'
			}
		},
		switch_primary : {
			method : 'PUT',
			url : '/api/zookeepers/primary',
			params : {
				zookeeperEnsembleId : '@zookeeperEnsembleId'
			}
		},
		update : {
			method : 'PUT'
		},
		initialize : {
			method : 'POST',
			url : '/api/zookeepers/initialize',
			params : {
				zookeeperEnsembleId : '@zookeeperEnsembleId'
			}
		},
		stopLeaseAssigning : {
			method : 'POST',
			url : '/api/zookeepers/leaseAssigning/stop',
			params : {
				serverId : '@serverId'
			}
		},
		stopLeaseAssigningForAllMetaServers : {
			method : 'POST',
			url : '/api/zookeepers/leaseAssigning/stopAll'
		},
		startLeaseAssigning : {
			method : 'POST',
			url : '/api/zookeepers/leaseAssigning/start',
			params : {
				serverId : '@serverId'
			}
		},
		startLeaseAssignings : {
			method : 'POST',
			url : '/api/zookeepers/leaseAssigning/startAll'
		},
		pauseAndSwitchZk : {
			method : 'POST',
			url : '/api/zookeepers/pauseAndSwitch',
			params : {
				serverId : '@serverId'
			}
		},
		pauseAndSwitchZkForAllServers : {
			method : 'POST',
			url : '/api/zookeepers/pauseAndSwitch/all'
		},
		resume : {
			method : 'POST',
			url : '/api/zookeepers/resume',
			params : {
				serverId : '@serverId'
			}
		},
		resumeAllServers : {
			method : 'POST',
			url : '/api/zookeepers/resume/all'
		},
		getRunningBrokers : {
			method : 'GET',
			url : '/api/zookeepers/runningBrokers'
		},
		pauseAndSwitchZkForSelf : {
			method : 'POST',
			url : '/api/zookeepers/pauseAndSwitch/self'
		},
		resumeSelf : {
			method : 'POST',
			url : '/api/zookeepers/resume/self'
		}
	})

	var metaServerResource = $resource("/api/servers", {}, {})

	var endpointResource = $resource("/api/endpoints", {}, {})

	$scope.zookeeperEnsembles = [];
	// $scope.currentPage = 'zookeeperEnsembles';
	$scope.currentPage = 'zookeeperMigration';
	$scope.currentZookeeperEnsemble = null;
	$scope.notCurrentZookeeperEnsembles = [];
	$scope.ops = [ true, false ];
	$scope.newZookeeperEnsemble = {
		primary : false
	}

	zookeeperResource.query(function(result) {
		$scope.zookeeperEnsembles = result;
		for (var i = 0; i < $scope.zookeeperEnsembles.length; i++) {
			if ($scope.zookeeperEnsembles[i].primary) {
				$scope.currentZookeeperEnsemble = $scope.zookeeperEnsembles[i];
			} else {
				$scope.notCurrentZookeeperEnsembles.push($scope.zookeeperEnsembles[i]);
			}
		}
	})

	$scope.deletezookeeperEnsemble = function(zookeeperEnsemble) {
		if (zookeeperEnsemble.primary) {
			show_op_info.show("不能删除优先集群！请先更换优先集群再删除。", false);
			return;
		}
		bootbox.confirm("确认删除ZookeeperEnsemble: " + zookeeperEnsemble.name + "(" + zookeeperEnsemble.connectionString + ")?", function(result) {
			if (result) {
				zookeeperResource.deleteZookeeperEnsemble({
					"zookeeperEnsembleId" : zookeeperEnsemble.id
				}, function(remove_result) {
					show_op_info.show("删除成功", true);
					zookeeperResource.query(function(result) {
						$scope.zookeeperEnsembles = result;
					})
				}, function(error_result) {
					show_op_info.show("删除失败: " + error_result.data, false);
				});
			}
		});
	}

	$scope.updatezookeeperEnsemble = function(zookeeperEnsemble) {
		if (zookeeperEnsemble.primary) {
			show_op_info.show("不能修改优先集群！请先更换优先集群再修改。", false);
			return;
		}
		bootbox.confirm("确认保存ZookeeperEnsemble: " + zookeeperEnsemble.name + "(" + zookeeperEnsemble.connectionString + ")?", function(result) {
			if (result) {
				zookeeperResource.update(zookeeperEnsemble, function(update_result) {
					show_op_info.show("更新成功", true);
					zookeeperResource.query(function(result) {
						$scope.zookeeperEnsembles = result;
					})
				}, function(error_result) {
					show_op_info.show("更新失败: " + error_result.data, false);
				});
			}
		});
	}
	$scope.addzookeeperEnsemble = function(zookeeperEnsemble) {
		if (zookeeperEnsemble.name == null || zookeeperEnsemble.name == "") {
			show_op_info.show("名称不能为空！", false);
			return;
		}
		if (zookeeperEnsemble.connectionString == null || zookeeperEnsemble.connectionString == "") {
			show_op_info.show("连接串不能为空！", false);
			return;
		}
		if (zookeeperEnsemble.idc == null || zookeeperEnsemble.idc == "") {
			show_op_info.show("IDC不能为空！", false);
			return;
		}

		for (var i = 0; i < $scope.zookeeperEnsembles.length; i++) {
			var tempZK = $scope.zookeeperEnsembles[i];
			if (tempZK.name == zookeeperEnsemble.name) {
				show_op_info.show("已经存在zookeeperEnsemble：" + zookeeperEnsemble.name + "，请更换名称", false);
				return;
			}
		}
		bootbox.confirm("确认添加ZookeeperEnsemble: " + zookeeperEnsemble.name + "(" + zookeeperEnsemble.connectionString + ")?", function(result) {
			if (result) {
				zookeeperResource.save(zookeeperEnsemble, function(update_result) {
					show_op_info.show("添加成功", true);
					zookeeperResource.query(function(result) {
						$scope.zookeeperEnsembles = result;
					})
				}, function(error_result) {
					show_op_info.show("添加失败: " + error_result.data, false);
				});
			}
		});
	}

	// migration
	$scope.stepStatus = {
		step1 : false,
		step2 : false,
		step3 : false,
		step4 : false,
		step5 : false,
		step6 : false,
		step7 : false

	}
	$scope.checkStatus = {
		checking : "checking",
		pedding : "pedding",
		success : "success",
		fail : "fail"
	}
	$scope.targetZookeeperEnsemble = null;
	$scope.metaServers = [];

	function setAllMetaServerCheckStatusToFail(checkAttr) {
		for (var row = 0; row < $scope.metaServers.length; row++) {
			for (var i = 0; i < $scope.metaServers[row].length; i++) {
				$scope.metaServers[row][i][checkAttr] = $scope.checkStatus.fail;
			}
		}
	}

	function setAllMetaServerCheckStatusToChecking(checkAttr) {
		for (var row = 0; row < $scope.metaServers.length; row++) {
			for (var i = 0; i < $scope.metaServers[row].length; i++) {
				$scope.metaServers[row][i][checkAttr] = $scope.checkStatus.checking;
			}
		}
	}

	function setAllMetaServerCheckStatus(checkAttr, checkResult) {
		for (var row = 0; row < $scope.metaServers.length; row++) {
			for (var i = 0; i < $scope.metaServers[row].length; i++) {
				if (checkResult.hasOwnProperty($scope.metaServers[row][i].id)) {
					if (checkResult[$scope.metaServers[row][i].id].success) {
						$scope.metaServers[row][i][checkAttr] = $scope.checkStatus.success;
					} else {
						$scope.metaServers[row][i][checkAttr] = $scope.checkStatus.fail;
					}
				} else {
					$scope.metaServers[row][i][checkAttr] = $scope.checkStatus.fail;
				}
			}
		}
	}

	function setMetaServerCheckStatus(metaServer, checkAttr, checkResult) {
		if (checkResult.success) {
			metaServer[checkAttr] = $scope.checkStatus.success;
		} else {
			metaServer[checkAttr] = $scope.checkStatus.fail;
		}
	}

	function checkAllMetaServersCheckStatusAreSuccess(checkAttr) {
		for (var row = 0; row < $scope.metaServers.length; row++) {
			for (var i = 0; i < $scope.metaServers[row].length; i++) {
				if ($scope.metaServers[row][i][checkAttr] != $scope.checkStatus.success) {
					return false;
				}
			}
		}
		return true;
	}

	// ************* STEP 1*************************
	$scope.initlizeZookeeperEnsemble = function(zookeeperEnsemble) {
		$scope.targetZookeeperEnsemble = zookeeperEnsemble;
		zookeeperResource.initialize({
			"zookeeperEnsembleId" : $scope.targetZookeeperEnsemble.id
		}, function(result) {
			$scope.stepStatus.step1 = true;
			show_op_info.show("初始化zookeeper集群:" + zookeeperEnsemble.name + "(" + zookeeperEnsemble.connectionString + ")成功！", true);
		}, function(result) {
			show_op_info.show("初始化zookeeper集群:" + zookeeperEnsemble.name + "(" + zookeeperEnsemble.connectionString + ")失败！" + result.data, false);
		});
	}

	$scope.getZookeeperEnsembleLabel = function(zk) {
		return zk.name + " (" + zk.connectionString + ")";
	}

	// ************* STEP 2*************************
	metaServerResource.query(function(result) {
		var rowSize = 4;
		var currentRow = [];
		$scope.metaServers.push(currentRow);
		for (var i = 0; i < result.length; i++) {
			result[i].stopLeaseAssigning = $scope.checkStatus.pedding;
			result[i].startLeaseAssigning = $scope.checkStatus.pedding;
			result[i].disconnectToZk = $scope.checkStatus.pedding;
			result[i].connectToZk = $scope.checkStatus.pedding;
			if (currentRow.length >= rowSize) {
				currentRow = [];
				$scope.metaServers.push(currentRow);
			}
			currentRow.push(result[i]);
		}
	})

	$scope.stopLeaseAssigning = function(metaServer) {
		metaServer.stopLeaseAssigning = $scope.checkStatus.checking;
		zookeeperResource.stopLeaseAssigning({
			"serverId" : metaServer.id
		}, function(result) {
			setMetaServerCheckStatus(metaServer, "stopLeaseAssigning", result);

			if (allLeaseAssigningsStoped()) {
				$scope.stepStatus.step2 = true;
			}

		}, function(result) {
			metaServer.stopLeaseAssigning = $scope.checkStatus.fail;
		})

	}

	$scope.stopLeaseAssignings = function() {
		setAllMetaServerCheckStatusToChecking("stopLeaseAssigning");

		zookeeperResource.stopLeaseAssigningForAllMetaServers(function(result) {
			setAllMetaServerCheckStatus("stopLeaseAssigning", result);

			if (allLeaseAssigningsStoped()) {
				$scope.stepStatus.step2 = true;
			}

		}, function(result) {
			setAllMetaServerCheckStatusToFail("stopLeaseAssigning");
		})

	}

	$scope.stopLeaseAssigningsForFaildServers = function() {
		for (var row = 0; row < $scope.metaServers.length; row++) {
			for (var i = 0; i < $scope.metaServers[row].length; i++) {
				if ($scope.metaServers[row][i].stopLeaseAssigning != $scope.checkStatus.success) {
					$scope.stopLeaseAssigning($scope.metaServers[row][i]);
				}
			}
		}
	}

	function allLeaseAssigningsStoped() {
		return checkAllMetaServersCheckStatusAreSuccess("stopLeaseAssigning");
	}

	// ************* STEP 3*************************
	$scope.switchPrimaryZkToTarget = function() {
		zookeeperResource.switch_primary({
			"zookeeperEnsembleId" : $scope.targetZookeeperEnsemble.id
		}, function(result) {
			show_op_info.show("更新 primary zookeeper ensemble 成功！", true);
			zookeeperResource.query(function(result) {
				$scope.zookeeperEnsembles = result;
				for (var i = 0; i < $scope.zookeeperEnsembles.length; i++) {
					if ($scope.zookeeperEnsembles[i].primary) {
						$scope.currentZookeeperEnsemble = $scope.zookeeperEnsembles[i];
					} else {
						$scope.notCurrentZookeeperEnsembles.push($scope.zookeeperEnsembles[i]);
					}
				}
				if ($scope.targetZookeeperEnsemble.connectionString == $scope.currentZookeeperEnsemble.connectionString) {
					$scope.stepStatus.step3 = true;
				}
			})
		}, function(result) {
			show_op_info.show("更新 primary zookeeper ensemble 失败！" + result.data, false);
		})

	}

	// ************* STEP 4*************************
	$scope.portal = {};
	$scope.portal.disconnectToZk = $scope.checkStatus.pedding;
	$scope.portal.connectToZk = $scope.checkStatus.pedding;

	$scope.pauseAndSwitchZkForSelf = function() {
		$scope.portal.disconnectToZk = $scope.checkStatus.checking;
		zookeeperResource.pauseAndSwitchZkForSelf(function(result) {
			$scope.portal.disconnectToZk = $scope.checkStatus.success;

			if (allDisconnectToZk()) {
				$scope.stepStatus.step4 = true;
			}
		}, function(result) {
			$scope.portal.disconnectToZk = $scope.checkStatus.fail;
		})
	}

	$scope.pauseAndSwitchZk = function(metaServer) {
		metaServer.disconnectToZk = $scope.checkStatus.checking;
		zookeeperResource.pauseAndSwitchZk({
			"serverId" : metaServer.id
		}, function(result) {
			setMetaServerCheckStatus(metaServer, "disconnectToZk", result);

			if (allDisconnectToZk()) {
				$scope.stepStatus.step4 = true;
			}

		}, function(result) {
			metaServer.disconnectToZk = $scope.checkStatus.fail;
		})
	}

	$scope.pauseAndSwitchZkForAllServers = function() {
		setAllMetaServerCheckStatusToChecking("disconnectToZk");

		zookeeperResource.pauseAndSwitchZkForAllServers(function(result) {
			setAllMetaServerCheckStatus("disconnectToZk", result);

			if (allDisconnectToZk()) {
				$scope.stepStatus.step4 = true;
			}

		}, function(result) {
			setAllMetaServerCheckStatusToFail("disconnectToZk");
		})
	}

	$scope.pauseAndSwitchZkForFailedServers = function() {
		for (var row = 0; row < $scope.metaServers.length; row++) {
			for (var i = 0; i < $scope.metaServers[row].length; i++) {
				if ($scope.metaServers[row][i].disconnectToZk != $scope.checkStatus.success) {
					$scope.pauseAndSwitchZk($scope.metaServers[row][i]);
				}
			}
		}
	}

	function allDisconnectToZk() {
		return $scope.portal.disconnectToZk == $scope.checkStatus.success && checkAllMetaServersCheckStatusAreSuccess("disconnectToZk");
	}

	// ************* STEP 5*************************
	$scope.resumeSelf = function() {
		$scope.portal.connectToZk = $scope.checkStatus.checking;
		zookeeperResource.resumeSelf(function(result) {
			$scope.portal.connectToZk = $scope.checkStatus.success;

			if (allConnectToZk()) {
				$scope.stepStatus.step5 = true;
			}
		}, function(result) {
			$scope.portal.connectToZk = $scope.checkStatus.fail;
		})

	}

	$scope.resume = function(metaServer) {
		metaServer.connectToZk = $scope.checkStatus.checking;
		zookeeperResource.resume({
			"serverId" : metaServer.id
		}, function(result) {
			setMetaServerCheckStatus(metaServer, "connectToZk", result);

			if (allConnectToZk()) {
				$scope.stepStatus.step5 = true;
			}

		}, function(result) {
			metaServer.connectToZk = $scope.checkStatus.fail;
		})
	}

	$scope.resumeAllServers = function() {
		setAllMetaServerCheckStatusToChecking("connectToZk");

		zookeeperResource.resumeAllServers(function(result) {
			setAllMetaServerCheckStatus("connectToZk", result);

			if (allConnectToZk()) {
				$scope.stepStatus.step5 = true;
			}

		}, function(result) {
			setAllMetaServerCheckStatusToFail("connectToZk");
		})
	}

	$scope.resumeFailedServers = function() {
		for (var row = 0; row < $scope.metaServers.length; row++) {
			for (var i = 0; i < $scope.metaServers[row].length; i++) {
				if ($scope.metaServers[row][i].connectToZk != $scope.checkStatus.success) {
					$scope.resume($scope.metaServers[row][i]);
				}
			}
		}
	}

	function allConnectToZk() {
		return $scope.portal.connectToZk == $scope.checkStatus.success && checkAllMetaServersCheckStatusAreSuccess("connectToZk");
	}

	// ************* STEP 6*************************
	$scope.startLeaseAssigning = function(metaServer) {
		metaServer.startLeaseAssigning = $scope.checkStatus.checking;
		zookeeperResource.startLeaseAssigning({
			"serverId" : metaServer.id
		}, function(result) {
			setMetaServerCheckStatus(metaServer, "startLeaseAssigning", result);

			if (allLeaseAssigningsStarted()) {
				$scope.stepStatus.step6 = true;
			}

		}, function(result) {
			metaServer.startLeaseAssigning = $scope.checkStatus.fail;
		})
	}

	$scope.startLeaseAssignings = function() {
		setAllMetaServerCheckStatusToChecking("startLeaseAssigning");

		zookeeperResource.startLeaseAssignings(function(result) {
			setAllMetaServerCheckStatus("startLeaseAssigning", result);

			if (allLeaseAssigningsStarted()) {
				$scope.stepStatus.step6 = true;
			}

		}, function(result) {
			setAllMetaServerCheckStatusToFail("startLeaseAssigning");
		})
	}

	$scope.startLeaseAssigningsForFaildServers = function() {
		for (var row = 0; row < $scope.metaServers.length; row++) {
			for (var i = 0; i < $scope.metaServers[row].length; i++) {
				if ($scope.metaServers[row][i].startLeaseAssigning != $scope.checkStatus.success) {
					$scope.startLeaseAssigning($scope.metaServers[row][i]);
				}
			}
		}
	}

	function allLeaseAssigningsStarted() {
		return checkAllMetaServersCheckStatusAreSuccess("startLeaseAssigning");
	}

	// ************* STEP 7*************************
	$scope.endpoints = [];

	$scope.getRunningBrokers = function() {
		zookeeperResource.getRunningBrokers(function(result) {
			for (var row = 0; row < $scope.endpoints.length; row++) {
				for (var i = 0; i < $scope.endpoints[row].length; i++) {
					var endpointId = $scope.endpoints[row][i].host + ":" + $scope.endpoints[row][i].port;
					console.log(endpointId);
					if (result.hasOwnProperty(endpointId)) {
						$scope.endpoints[row][i].isRunning = $scope.checkStatus.success;
					} else {
						$scope.endpoints[row][i].isRunning = $scope.checkStatus.fail;
					}
				}
			}
		}, function(result) {
			show_op_info.show("检查runningBrokers失败！" + result.data, false);
		})
	}
	endpointResource.query(function(result) {
		var rowSize = 4;
		var currentRow = [];
		$scope.endpoints.push(currentRow);
		for (var i = 0; i < result.length; i++) {
			result[i].isRunning = $scope.checkStatus.pedding;
			if (currentRow.length >= rowSize) {
				currentRow = [];
				$scope.endpoints.push(currentRow);
			}
			currentRow.push(result[i]);
		}

	}, function(result) {
		show_op_info.show("获取Brokers列表失败！" + result.data, false);
	})

} ]);