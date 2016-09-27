angular.module('idcApp', [ 'ngResource', 'smart-table', 'xeditable', 'toggle-switch' ]).run(function(editableOptions) {
	editableOptions.theme = 'bs3';
}).controller('idcController', [ '$scope', '$resource', '$q', function($scope, $resource, $q) {

	var idcResource = $resource('/api/idcs', {}, {
		get_idc : {
			method : 'GET',
			url : '/api/idcs/:idc',
			params : {
				idc : '@idc'
			}
		},
		delete_idc : {
			method : 'DELETE',
			url : '/api/idcs/:idc',
			params : {
				idc : '@idc'
			}
		},
		switch_primary : {
			method : 'PUT',
			url : '/api/idcs/primary/:idc',
			params : {
				idc : '@idc'
			}
		},
		force_switch_primary : {
			method : 'PUT',
			url : '/api/idcs/primary/:idc',
			params : {
				idc : '@idc',
				force : '@force'
			}
		},
		enable_idc : {
			method : 'PUT',
			url : '/api/idcs/enable/:idc',
			params : {
				idc : '@idc'
			}
		},
		disable_idc : {
			method : 'PUT',
			url : '/api/idcs/disable/:idc',
			params : {
				idc : '@idc'
			}
		}
	})

	var endpointResource = $resource('/api/endpoints', {}, {
		'deleteEndpoint' : {
			method : 'DELETE',
			url : '/api/endpoints/:id'
		},
		'updateEndpoint' : {
			method : 'POST',
			url : '/api/endpoints/update/'
		}
	});

	var serverResource = $resource('/api/servers', {}, {
		'deleteServer' : {
			method : 'DELETE',
			url : '/api/servers/:id',
			params : {
				server : '@id'
			}
		},
		'update' : {
			method : 'PUT'
		}
	})

	$scope.idcs = [];
	$scope.currentIdc = null;
	$scope.endpoints = [];
	$scope.currentEndpoints = [];
	$scope.servers = [];
	$scope.currentServers = [];

	$scope.newIdc = {
		enabled : true,
		primary : false
	}

	$scope.newServer = {
		host : '127.0.0.1',
		port : 8080,
		enabled : true,
		idc : ""
	}

	$scope.endpointsTypes = [ "broker", "kafka" ];
	$scope.newEndpoint = {
		host : '127.0.0.1',
		type : 'broker',
		port : 4376,
		group : 'idle',
		idc : ""
	};
	$scope.ops = [ true, false ]

	function setCurrentIdc(idc) {
		$scope.currentIdc = idc;
		$scope.newEndpoint.idc = idc.name;
		$scope.newServer.idc = idc.name;
	}

	function setCurrentServers(idcName) {
		$scope.currentServers = [];
		for (var i = 0; i < $scope.servers.length; i = i + 1) {
			if ($scope.servers[i].idc == idcName) {
				$scope.currentServers.push($scope.servers[i]);
			}
		}
	}

	function setCurrentEndpoints(idcName) {
		$scope.currentEndpoints = [];
		for (var i = 0; i < $scope.endpoints.length; i = i + 1) {
			if ($scope.endpoints[i].idc == idcName) {
				$scope.currentEndpoints.push($scope.endpoints[i]);
			}
		}
	}

	$scope.switchCurrentIdc = function switchCurrentIdc(idc) {
		setCurrentIdc(idc);
		setCurrentServers(idc.name);
		setCurrentEndpoints(idc.name);
	}

	idcResource.query(function(result) {
		$scope.idcs = result;
		if ($scope.idcs.length > 0) {
			setCurrentIdc($scope.idcs[0]);
		}

		serverResource.query(function(result) {
			$scope.servers = result;
			if ($scope.currentIdc != null) {
				setCurrentServers($scope.currentIdc.name);
			}
		})

		endpointResource.query(function(result) {
			$scope.endpoints = result;
			if ($scope.currentIdc != null) {
				setCurrentEndpoints($scope.currentIdc.name);
			}
		})
	});

	$scope.addIdc = function addIdc(newIdc) {
		idcResource.save(newIdc).$promise.then(function(resultIdc) {
			show_op_info.show("新增成功, 名称: " + resultIdc.name, true);
			idcResource.query(function(result) {
				$scope.idcs = result;
				if ($scope.idcs.length > 0) {
					$scope.switchCurrentIdc($scope.idcs[0]);
				}
			});
		}, function(result) {
			show_op_info.show("新增失败: " + result.data, false);
		})

	}

	$scope.addEndpoint = function addEndpoint(newEndpoint) {
		endpointResource.save(newEndpoint).$promise.then(function(save_result) {
			show_op_info.show("新增成功, 名称: " + newEndpoint.id, true);
			endpointResource.query(function(result) {
				$scope.endpoints = result;
				if ($scope.currentIdc != null) {
					setCurrentEndpoints($scope.currentIdc.name);
				}
			});
		}, function(error_result) {
			show_op_info.show("新增失败: " + error_result.data, false);
		});
	};

	$scope.deleteEndpoint = function deleteEndpoint(id) {
		bootbox.confirm("确认删除 Endpoint: " + id + "?", function(result) {
			if (result) {
				endpointResource.deleteEndpoint({
					"id" : id
				}, function(remove_result) {
					show_op_info.show("删除成功", true);
					endpointResource.query(function(result) {
						$scope.endpoints = result;
						if ($scope.currentIdc != null) {
							setCurrentEndpoints($scope.currentIdc.name);
						}
					});
				}, function(error_result) {
					show_op_info.show("删除失败: " + error_result.data, false);
				});
			}
		});
	};

	$scope.updateEndpoint = function updateEndpoint(row) {
		bootbox.confirm("确认保存 Endpoint: " + row.id + "?", function(result) {
			if (result) {
				endpointResource.updateEndpoint(row, function(result) {
					show_op_info.show("更新成功", true);
					endpointResource.query(function(result) {
						$scope.endpoints = result;
						if ($scope.currentIdc != null) {
							setCurrentEndpoints($scope.currentIdc.name);
						}
					});
				}, function(error_result) {
					show_op_info.show("更新失败: " + error_result.data, false);
					endpointResource.query(function(result) {
						$scope.endpoints = result;
						if ($scope.currentIdc != null) {
							setCurrentEndpoints($scope.currentIdc.name);
						}
					});
				});
			}
		});
	}

	$scope.switchPrimary = function(doSwitch) {
		if ($scope.currentIdc.primary) {
			show_op_info.show("请选择您想要置为primary的idc, 然后置为primary！", false);
		} else {
			bootbox.confirm("确认将idc： " + $scope.currentIdc.name + "置为primary?", function(result) {
				if (result) {
					idcResource.switch_primary({
						"idc" : $scope.currentIdc.id
					}, function(result) {
						show_op_info.show("切换primary idc成功！当前primary idc: " + $scope.currentIdc.name, true);
						doSwitch();
						idcResource.query(function(result) {
							$scope.idcs = result;
						})
					}, function(result) {
						$scope.forceSwitchPrimary(doSwitch, result.data);
						// show_op_info.show("切换primary idc失败: " + result.data,
						// false);
					})
				}
			})
		}
	};

	$scope.forceSwitchPrimary = function(doSwitch, errdata) {
		if ($scope.currentIdc.primary) {
			show_op_info.show("请选择您想要置为primary的idc, 然后置为primary！", false);
		} else {
			bootbox.confirm(errdata + "<br><strong class='text-danger'>是否强制将idc： " + $scope.currentIdc.name + "置为primary?</strong>", function(result) {
				if (result) {
					idcResource.force_switch_primary({
						"idc" : $scope.currentIdc.id,
						"force" : true
					}, function(result) {
						show_op_info.show("切换primary idc成功！当前primary idc: " + $scope.currentIdc.name, true);
						doSwitch();
						idcResource.query(function(result) {
							$scope.idcs = result;
						})
					}, function(result) {
						show_op_info.show("切换primary idc失败: " + result.data, false);
					})
				}
			})
		}
	};

	$scope.switchEnabled = function(doSwitch) {
		if ($scope.currentIdc.enabled) {
			bootbox.confirm("确认要关闭idc： " + $scope.currentIdc.name + "?", function(result) {
				if (result) {
					idcResource.disable_idc({
						"idc" : $scope.currentIdc.id
					}, function(result) {
						show_op_info.show("关闭idc：" + $scope.currentIdc.name + "成功！", true);
						doSwitch();
						idcResource.query(function(result) {
							$scope.idcs = result;
						})
					}, function(result) {
						show_op_info.show("关闭idc失败: " + result.data, false);
					})
				}
			})

		} else {
			bootbox.confirm("确认要启用idc： " + $scope.currentIdc.name + "?", function(result) {
				if (result) {
					idcResource.enable_idc({
						"idc" : $scope.currentIdc.id
					}, function(result) {
						show_op_info.show("开启idc：" + $scope.currentIdc.name + "成功！", true);
						doSwitch();
						idcResource.query(function(result) {
							$scope.idcs = result;
						})
					}, function(result) {
						show_op_info.show("开启idc失败: " + result.data, false);
					})
				}
			})
		}
	};

	$scope.deleteIdc = function deleteIdc() {
		bootbox.confirm("确认要删除idc： " + $scope.currentIdc.name + "?", function(result) {
			if (result) {
				idcResource.delete_idc({
					"idc" : $scope.currentIdc.id
				}, function(result) {
					show_op_info.show("删除idc：" + $scope.currentIdc.name + "成功！", true);
					idcResource.query(function(result) {
						$scope.idcs = result;
						if ($scope.idcs.length > 0) {
							switchCurrentIdc($scope.idcs[0]);
						}
					})
				}, function(result) {
					show_op_info.show("删除idc失败: " + result.data, false);
				})
			}
		})
	}

	$scope.addServer = function addServer(newServer) {
		serverResource.save(newServer).$promise.then(function(save_result) {
			show_op_info.show("新增成功, 名称: " + newServer.id, true);
			serverResource.query(function(result) {
				$scope.servers = result;
				if ($scope.currentIdc != null) {
					setCurrentServers($scope.currentIdc.name);
				}
			});
		}, function(error_result) {
			show_op_info.show("新增失败: " + error_result.data, false);
		});
	};

	$scope.deleteServer = function deleteServer(id) {
		bootbox.confirm("确认删除 Server: " + id + "?", function(result) {
			if (result) {
				serverResource.deleteServer({
					"id" : id
				}, function(remove_result) {
					show_op_info.show("删除成功", true);
					serverResource.query(function(result) {
						$scope.servers = result;
						if ($scope.currentIdc != null) {
							setCurrentServers($scope.currentIdc.name);
						}
					});
				}, function(error_result) {
					show_op_info.show("删除失败: " + error_result.data, false);
				});
			}
		});
	};

	$scope.updateServer = function updateServer(row) {
		bootbox.confirm("确认保存 Server: " + row.id + "?", function(result) {
			if (result) {
				serverResource.update(row, function(result) {
					show_op_info.show("更新成功", true);
					serverResource.query(function(result) {
						$scope.servers = result;
						if ($scope.currentIdc != null) {
							setCurrentServers($scope.currentIdc.name);
						}
					});
				}, function(error_result) {
					show_op_info.show("更新失败: " + error_result.data, false);
					serverResource.query(function(result) {
						$scope.servers = result;
						if ($scope.currentIdc != null) {
							setCurrentServers($scope.currentIdc.name);
						}
					});
				});
			}
		});
	}

} ])
