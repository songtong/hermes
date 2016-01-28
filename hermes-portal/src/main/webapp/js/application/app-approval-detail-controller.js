application_module.controller('app-approval-detail-controller', [ '$scope', '$routeParams', '$resource', 'ApplicationService', '$location', '$window', '$q', function($scope, $routeParams, $resource, ApplicationService, $location, $window, $q) {
	var storage_resource = $resource('/api/storages', {}, {
		'get_storage' : {
			method : 'GET',
			isArray : true,
		}
	});
	$scope.order_opts = [ true, false ];
	$scope.datasources = [];
	$scope.new_comment = "";
	ApplicationService.get_generated_application($routeParams['id']).then(function(result) {
		$scope.application = result["key"];
		$scope.view = result["value"];
		console.log($scope.view.partitions);
		updatePageType($scope.application["type"]);
		if ($scope.application["type"] == 0 || $scope.application["type"] == 2) {// create
			$scope.defaultReadDS = $scope.view.partitions[0].readDatasource;
			$scope.defaultWriteDS = $scope.view.partitions[0].writeDatasource;
			if ($scope.view["storageType"] == "mysql") {
				storage_resource.get_storage({
					'type' : 'mysql'
				}, function(data) {
					for (var i = 0; i < data[0].datasources.length; i++) {
						$scope.datasources.push(data[0].datasources[i].id);
					}
					for (var i = 0; i < $scope.view.partitions.length; i++) {
						$scope.view.partitions[i].readDatasource = $scope.defaultReadDS;
						$scope.view.partitions[i].writeDatasource = $scope.defaultWriteDS;
					}
				});
				$scope.codec_types = [ 'json', 'cmessaging' ];
				$scope.endpoint_types = [ 'broker' ];

			} else {
				storage_resource.get_storage({
					'type' : 'kafka'
				}, function(data) {
					for (var i = 0; i < data[0].datasources.length; i++) {
						$scope.datasources.push(data[0].datasources[i].id);
					}
					for (var i = 0; i < $scope.view.partitions.length; i++) {
						$scope.view.partitions[i].readDatasource = $scope.defaultReadDS;
						$scope.view.partitions[i].writeDatasource = $scope.defaultWriteDS;
					}
				});
				$scope.properties = [ 'partitions', 'replication-factor', 'retention.bytes', 'retention.ms' ]
				$scope.endpoint_types = [ 'kafka', 'broker' ];
				$scope.codec_types = [ 'avro', 'json' ];
			}
		}
	});
	$scope.add_partition = function(view) {
		var new_partition = {};
		if (view.partitions.length == 0) {
			new_partition.readDatasource = $scope.defaultReadDS;
			new_partition.writeDatasource = $scope.defaultWriteDS;
		} else {
			new_partition.readDatasource = view.partitions[view.partitions.length - 1].readDatasource;
			new_partition.writeDatasource = view.partitions[view.partitions.length - 1].writeDatasource;
		}
		view.partitions.push(new_partition);
	};

	$scope.delete_partition = function(view, index) {
		view.partitions.splice(index, 1);
	};

	$scope.add_property = function(view) {
		var new_property = {};
		view.properties.push(new_property);
	};

	$scope.delete_property = function(view, index) {
		view.properties.splice(index, 1);
	};

	function updatePageType(typeCode) {
		switch (typeCode) {
		case 0:
			$scope.currentPageType = "Topic创建";
			break;
		case 1:
			$scope.currentPageType = "Consumer创建";
			break;
		case 2:
			$scope.currentPageType = "Topic修改";
			break;
		case 3:
			$scope.currentPageType = "Consumer修改";
			break;
		default:
			console.log("default");
		}
	}

	function get_new_consuemr(view, topics, i) {
		var consumer = {};
		consumer.topicName = topics[i];
		consumer.orderedConsume = view.orderedConsume;
		consumer.appId = view.appId;
		consumer.groupName = view.groupName;
		consumer.retryPolicy = view.retryPolicy;
		consumer.ackTimeoutSeconds = view.ackTimeoutSeconds;
		consumer.owner1 = view.owner1;
		consumer.owner2 = view.owner2;
		consumer.phone1 = view.phone1;
		consumer.phone2 = view.phone2;
		return consumer;
	}

	$scope.topicDetailForm = {
		createTopicOnFws : true,
		createTopicOnUat : true,
		createTopicOnProd : true,
		buildMetaOnFws : true,
		buildMetaOnUat : true,
		buildMetaOnProd : true,
		progressInfo : "",
		totalTaskCount : 7,
		terminated : false,
		setProgreaaBarValue : function(value) {
			document.getElementById("progressbar").style.width = value;
		},
		disablePermitTopicButton : function() {
			document.getElementById("permitTopicButton").disabled = true;
		},
		enablePermitTopicButton : function() {
			document.getElementById("permitTopicButton").disabled = false;
		},
		showProgressBar : function() {
			$("#progressBarprompt").show();
		},
		hideProgressBar : function() {
			$("#progressBarprompt").hide();
		},
		disactiveProgressBar : function() {
			$("#progressbar").removeClass("active");
		},
		activeProgressBar : function() {
			$("#progressbar").addClass("active");
		},
		setProgressStatus : function(progressInfo, value, taskCount) {
			var currentValue = (value + taskCount - 1) / this.totalTaskCount * 100 + "%";
			console.log(currentValue);
			this.progressInfo = progressInfo;
			this.setProgreaaBarValue(currentValue);
			if (currentValue == "100%") {
				this.disactiveProgressBar();
			}
		},
		initProgressBar : function() {
			this.showProgressBar();
			this.activeProgressBar();
			this.setProgreaaBarValue("0%");
			this.progressInfo = "";
			this.disablePermitTopicButton();
		}
	}

	function syncTopicToUat() {
		var delay = $q.defer();
		if (!$scope.topicDetailForm.createTopicOnUat) {
			$scope.topicDetailForm.setProgressStatus("同步topic至Uat环境:略过。", 1, 2);
			delay.resolve();
		} else {
			$scope.topicDetailForm.setProgressStatus("正在同步topic至Uat环境", 0.3, 2);
			ApplicationService.sync_topic($scope.uatTopicView, 'uat', false).then(function(result) {
				$scope.topicDetailForm.setProgressStatus("同步至Uat环境成功。", 1, 2);
				delay.resolve();
			}, function(result) {
				$scope.topicDetailForm.setProgressStatus("同步至Uat环境失败: " + result.data, 1, 2);
				delay.reject();
			})
		}
		return delay.promise;
	}

	function syncTopicToProd() {
		var delay = $q.defer();
		if (!$scope.topicDetailForm.createTopicOnProd) {
			$scope.topicDetailForm.setProgressStatus("同步topic至Prod环境:略过。", 1, 3);
			delay.resolve();
		} else {
			$scope.topicDetailForm.setProgressStatus("正在同步topic至Prod环境", 0.3, 3);
			ApplicationService.sync_topic($scope.ProdTopicView, 'prod', false).then(function(result) {
				$scope.topicDetailForm.setProgressStatus("同步至Prod环境成功。", 1, 3);
				delay.resolve();
			}, function(result) {
				$scope.topicDetailForm.setProgressStatus("同步至Prod环境失败: " + result.data, 1, 3);
				delay.reject();
			})
		}
		return delay.promise;
	}

	function createTopic() {
		var delay = $q.defer();
		if (!$scope.topicDetailForm.createTopicOnFws) {
			$scope.topicDetailForm.setProgressStatus("创建topic至Fws环境:略过。", 1, 1);
			delay.resolve();
		} else {
			$scope.topicDetailForm.setProgressStatus("正在Fws环境创建Topic...", 0.3, 1);
			ApplicationService.create_topic($scope.fwsTopicView).then(function(save_result) {
				if ($scope.view.storageType == 'kafka') {
					$scope.topicDetailForm.setProgressStatus("正在发布Topic至Kafka...", 0.6, 1);
					ApplicationService.deploy_topic($scope.view.name).then(function(response) {
						$scope.topicDetailForm.setProgressStatus("Topic创建成功！", 1, 1);
						delay.resolve();
					}, function(response) {
						$scope.topicDetailForm.setProgressStatus("发布Topic到Kafka失败!正在删除Topic...！", 0.8, 1);
						ApplicationService.remove_topic($scope.view.name).then(function(success_resp) {
							$scope.topicDetailForm.setProgressStatus("发布Topic到Kafka失败, 删除脏数据成功！", 1, 1);
							$scope.topicDetailForm.enablePermitTopicButton();
							delay.reject();
						}, function(error_resp) {
							$scope.topicDetailForm.setProgressStatus("发布Topic到Kafka失败, 删除脏数据失败！", 1, 1);
							$scope.topicDetailForm.enablePermitTopicButton();
							delay.reject();
						});
					});
				} else {
					$scope.topicDetailForm.setProgressStatus("Topic创建成功！", 1, 1);
					delay.resolve();
				}
			}, function(error_result) {
				$scope.topicDetailForm.setProgressStatus("创建topic失败：" + error_result.data, 1, 1);
				$scope.topicDetailForm.enablePermitTopicButton();
				delay.reject();
			});
		}
		return delay.promise;
	}

	function passApplication() {
		var delay = $q.defer();
		$scope.topicDetailForm.setProgressStatus("正在更新表单...", 0.3, 7);
		ApplicationService.pass_application($scope.application.id, $scope.new_comment, "Hermes").then(function(result) {
			$scope.topicDetailForm.setProgressStatus("表单更新成功", 1, 7);
			delay.resolve();
			// $window["location"].replace("/console/topic#detail/mysql/mysql/"
			// + $scope.view.name);
		}, function(result) {
			$scope.topicDetailForm.setProgressStatus("表单更新失败", 1, 7);
			delay.reject(result);
		});
		return delay.promise;

	}
	// $scope.topicDetailForm.showProgressBar();
	$scope.permit_topic_application = function() {
		$scope.topicDetailForm.initProgressBar();
		// 在fws创建topic
		createTopic().then(function(result) {
			console.log("create success.")
			syncTopicToUat().then(function(result) {
				console.log("sync to uat success.")
				syncTopicToProd().then(function() {
					console.log("sync to prod success.")
					// build meta
					passApplication();

				})
			})
		});

	}
	function clone(myObj) {
		if (typeof (myObj) != 'object' || myObj == null)
			return myObj;
		var newObj = new Object();
		for ( var i in myObj) {
			newObj[i] = clone(myObj[i]);
		}
		return newObj;
	}

	$scope.initTopicInfo = function() {
		$scope.fwsTopicView = $scope.view;
		$scope.uatTopicView = clone($scope.view);
		$scope.prodTopicView = clone($scope.view);
		console.log($scope.fwsTopicView);
		console.log($scope.uatTopicView);
		console.log($scope.prodTopicView);
	}

	$scope.permit_consumer_application = function() {
		var topics = $scope.view.topicName.split(",");
		console.log(topics);
		show_op_info.show("正在创建Consumer...", true);
		var consumers = [];
		for (var i = 0; i < topics.length; i++) {
			consumers.push(get_new_consuemr($scope.view, topics, i));
		}
		ApplicationService.add_consumers(consumers).then(function(result1) {
			ApplicationService.pass_application($scope.application.id, $scope.new_comment, "Hermes").then(function(result) {
				$scope.application = result;
				show_op_info.show("为Topic：" + result1.success_topics + "创建Consumer成功！(共" + result1.success_topics.length + "个成功，" + (topics.length - result1.success_topics.length) + "个失败)，表单状态修改成功", true);
			}, function(result) {
				show_op_info.show("为Topic：" + result1.success_topics + "创建Consumer成功！(共" + result1.success_topics.length + "个成功，" + (topics.length - result1.success_topics.length) + "个失败)，表单状态修改失败", false);
			});

		}, function(result) {
			show_op_info.show("新增 consumer失败! " + error_result.data, false);
			document.getElementById("permitConsumerButton").disabled = false;
		});
	}

	$scope.reject_application = function() {
		document.getElementById("rejectButton").disabled = "disabled";
		console.log($scope.application.comment);
		ApplicationService.reject_application($scope.application.id, $scope.new_comment, "Hermes").then(function(result) {
			show_op_info.show("拒绝申请成功", true);
			document.getElementById("rejectButton").disabled = false;
			$scope.application = result;
		}, function(result) {
			document.getElementById("rejectButton").disabled = false;
			show_op_info.show("拒绝申请失败", false);
		});
	}
} ])