application_module.controller('app-approval-detail-controller', [ '$scope', '$routeParams', '$resource', 'ApplicationService', '$location', '$window', '$q', '$filter', '$resource', 'watcher', 'TopicSync', 'clone', 'cache', 'promiseChain', 'user', function($scope, $routeParams, $resource, ApplicationService, $location, $window, $q, $filter, $resource, watcher, TopicSync, clone, cache, promiseChain, user) {
	// Default options.
	$scope.env = 'fws';
	$scope.storageTypes = [ 'mysql', 'kafka' ];
	$scope.languageTypes = [ 'java', '.net' ];
	$scope.views = {};
	
	$scope.order_opts = [ true, false ];
	$scope.datasources = {};
	$scope.comment = "";
	$scope.envInvolved = [];
	
	$scope.tags = {};
	$scope.datasourcesTags = {};
	$scope.selectedTags = {};
	$scope.datasourceCandidate = {};
	$scope.brokerGroups = {};

	// Use promise chain to fetch application.
	promiseChain.add({
		func: ApplicationService.get_application,
		args: [$routeParams['id']],
		success: function(result) {
			$scope.application = result;
			decodeComment();
			if ($scope.application.type == 0 || $scope.application.type == 2) {
				$scope.type = 'topic';
			} else {
				$scope.type = 'consumer';
			}
			if ($scope.application.storageType == 'mysql') {
				$scope.codec_types = [ 'json', 'cmessaging' ];
				$scope.endpoint_types = [ 'broker' ];
			} else {
				$scope.properties = [ 'partitions', 'replication-factor', 'retention.bytes', 'retention.ms' ];
				$scope.endpoint_types = [ 'kafka', 'broker' ];
				$scope.codec_types = [ 'avro', 'json' ];
			}
			$scope.$broadcast('initialized');
		}
	}).finish();

	$scope.addPartition = function() {
		var partition = {};
		if ($scope.view.partitions.length == 0) {
			partition.readDatasource = $scope.datasources[$scope.env][0];
			partition.writeDatasource = $scope.datasources[$scope.env][0];
		} else {
			partition.readDatasource = $scope.view.partitions[$scope.view.partitions.length - 1].readDatasource;
			partition.writeDatasource = $scope.view.partitions[$scope.view.partitions.length - 1].writeDatasource;
		}
		$scope.view.partitions.push(partition);
	};

	$scope.deletePartition = function(index) {
		$scope.view.partitions.splice(index, 1);
	};

	$scope.addProperty = function() {
		$scope.view.properties.push({});
	};

	$scope.deleteProperty = function(index) {
		$scope.view.properties.splice(index, 1);
	};
	
	function handleError(result) {
		$scope.$broadcast('progress-done', 'syncProgressBar', function(){
			$scope.$broadcast('alert-error', 'syncAlert', result.data);
		});
	}
	
	function handleSuccess(msg) {
		$scope.$broadcast('progress-done', 'syncProgressBar', function(){
			$scope.$broadcast('alert-success', 'syncAlert', msg, 'Success');
		});
	}
	
	function encodeComment() {
		if ($scope.application && $scope.comment) {
			$scope.application.comment.push({
				createdTime: $filter('date')(new Date(), 'yyyy-MM-dd HH:mm:ss'),
				author: user.sn,
				comment: $scope.comment
			});
		}
		return JSON.stringify($scope.application.comment);
	}
	
	function decodeComment() {
		if ($scope.application.comment) {
			try {
				$scope.application.comment = JSON.parse($scope.application.comment);
			} catch (e) {
				// ignore for the case comment is old-style.
			}
			
			// Compatible with the old-style comment.
			if (!($scope.application.comment instanceof Array)) {
				$scope.application.comment = [{
					createdTime: $filter('date')(new Date($scope.application.lastModifiedTime), 'yyyy-MM-dd HH:mm:ss'),
					author: $scope.application.approver,
					comment: $scope.application.comment
				}];
			}
		} else {
			$scope.application.comment = [];
		}
		
		console.log($scope.application.comment);
	}
	
	$scope.updateSelection = function() {
		var selectionResult = {};
		var selectedTags = $scope.selectedTags[$scope.env];
		var datasourcesTags = $scope.datasourcesTags[$scope.env];
		
		for (var group in selectedTags) {
			// Ignore the case the selection is useless.
			if (!selectedTags[group]) {
				return;
			}
			
			// Selected tag on one tag group.
			var tagId = selectedTags[group].id;

			// Check whether having datasource attached to this tag.
			for (var ds in datasourcesTags) {
				datasourcesTags[ds].forEach(function(tag, index){
					if (tag.id == tagId) {
						if (!selectionResult[ds]) {
							selectionResult[ds] = 0;
						}
						selectionResult[ds]++;
					}
				});
			}
		}
		
		var max = 0;
		var datasource = null;
		for (var ds in selectionResult) {
			if (selectionResult[ds] > max) {
				datasource = ds;
			}
		}
		
		$scope.datasourceCandidate[$scope.env] = datasource? datasource: 'No Eligible';
	};

	$scope.createTopic = function() {
		// Records the env in case it changes.
		var env = $scope.env;
	
		// Set timeout to 10 seconds, and set maximum to 80 if it's a mysql topic, otherwise 30.
		$scope.$broadcast('progress-random', 'syncProgressBar', 
				$scope.view.storageType == 'kafka'? 3000: 10000, $scope.view.storageType == 'kafka'? 30: 80);
		
		// Register a watcher to sync two promise-chains.
		var finishWatcher = watcher.register({
			context: {count: 0},
			step: function(){
				if ($scope.view.storageType == 'mysql') {
					return true;
				} else if (++this.count == 2) {
					return true;
				}
				return false;
			},
			handlers: [function() {
				if ($scope.application.status == 2) {
					ApplicationService.update_application_status($scope.application.id, 3, encodeComment(), user.sn, $scope.application).then(function(result) {
						$scope.application = result;
						decodeComment();
						handleSuccess('Done initializing topic on env: ' + env);
					}, handleError);
					
				} else if($scope.application.status == 6){
					ApplicationService.update_application_status($scope.application.id, 7, encodeComment(), user.sn, $scope.application).then(function(result) {
						$scope.application = result;
						decodeComment();
						handleSuccess('Done initializing topic on env: ' + env);
					}, handleError);
					
				}
				else {
					handleSuccess('Done initializing topic on env: ' + env);
				}
			}, function() {
				$scope.envInvolved.push(env);
			}]
		});
		
		var topicChain = promiseChain.newBorn();
		topicChain.add({
			func:TopicSync.createTopic,
			args: [env, $scope.view],
			success: function(result) {
				$scope.views[env] = result;
				$scope.view = $scope.views[env];
				
				if($scope.view.storageType == 'mysql') {
					$scope.$broadcast('progress', 'syncProgressBar', {percentage: 90, msg: 'Topic created on env: ' + $scope.env});
				} else {
					$scope.$broadcast('progress', 'syncProgressBar', {percentage: 50, msg: 'Topic created on env: ' + $scope.env});
				}
				finishWatcher.step();
			},
			error: handleError
		});
		
		if ($scope.view.storageType == 'kafka') {
			topicChain.add({
				func:TopicSync.deployKafkaTopic,
				args: [env, {name: $scope.view.name}],
				success: function(result) {
					$scope.$broadcast('progress', 'syncProgressBar', {percentage: 90, msg: 'Topic deployed on env: ' + $scope.env});
					finishWatcher.step();
				},
				error: handleError
			});
		}
		
		topicChain.finish();
	};
	
	$scope.createConsumer = function() {
		// Records the env in case it changes.
		var env = $scope.env;
		
		$scope.$broadcast('progress-random', 'syncProgressBar', 5000);
		var topicNames = $scope.application.topicName.split(',');
		
		// Register a watcher to sync two promise-chains.
		var finishWatcher = watcher.register({
			context: {count: 0},
			step: function(){
				if (++this.count == topicNames.length) {
					return true;
				}
				$scope.$broadcast('progress-step', 'syncProgressBar', 100 / topicNames.length * this.count);
			},
			handlers: [function() {
				if ($scope.application.status == 2) {
					ApplicationService.update_application_status($scope.application.id, 3, encodeComment(), user.sn).then(function(result) {
						$scope.application = result;
						decodeComment();
						handleSuccess('Done initializing consumer on env: ' + env);
					}, handleError);
				} else if($scope.application.status == 6){
					ApplicationService.update_application_status($scope.application.id, 7, encodeComment(), user.sn, $scope.application).then(function(result) {
						$scope.application = result;
						decodeComment();
						handleSuccess('Done initializing topic on env: ' + env);
					}, handleError);
					
				}else {
					handleSuccess('Done initializing consumer on env: ' + env);
				}
			}]
		});

		var promises = [];
		for (var index in topicNames) {
			var view = clone($scope.view);
			view['topicName'] = topicNames[index];
			promises.push({
				func: TopicSync.addConsumer,
				args: [env, view],
				success: function(result) {
					$scope.views[env] = result;
					$scope.view = $scope.views[env];
					
					finishWatcher.step();
				},
				error: handleError
			});
		}
		
		promiseChain.newBorn().add(promises).finish();
	};
	
	$scope.passApplication = function passApplication() {
		
		ApplicationService.pass_application($scope.application.id, encodeComment(), user.sn, $scope.application).then(function(result) {
			$scope.application = result;
			decodeComment();
			// When the application is passed, invoke api to get dynamic view.
			$scope.openSyncModal();
		}, function(result) {
		});
	};
	
	$scope.openSyncModal = function() {
		$scope.$broadcast('progress-random', 'modalProgressBar', 5000);
		
		// Register a watcher to sync two promise-chains.
		var finishWatcher = watcher.register({
			context: {count: 0},
			step: function(){
				if (++this.count == 2) {
					return true;
				}
			},
			handlers: [function() {
				console.log($scope.tags);
				$scope.$broadcast('progress-done', 'modalProgressBar', function(){
					$('#topicInfoModal').one('hidden.bs.modal', function(){
						if ($scope.envInvolved.length > 0) {
							$scope.$broadcast('alert-success', 'metaWarning');
						}
					}).modal();
				});
			}]
		});
		
		// Check whether topic exists on certain env.
		function checkOnEnv(env) {
			var remoteChain = promiseChain.newBorn();
			if ($scope.application["type"] == 0) {
				// Compose topic name.
				var topicName = $scope.application.productLine + '.' + $scope.application.entity + '.' + $scope.application.event;
				
				// For creation requests, call storage api to get list serving config modal view. 
				remoteChain.add({
					func: TopicSync.getStorage,
					args: [env, {type: $scope.application.storageType}],
					success: function(data) {
						var datasources = []
						for (var i = 0; i < data[0].datasources.length; i++) {
							datasources.push(data[0].datasources[i].id);
						}
						$scope.datasources[env] = datasources;
						
						$scope.$broadcast('progress', 'modalProgressBar', 'Fetched storages on env: ' + env);
					}
				}).add({
					func: TopicSync.getEndpoints,
					args: [env],
					success: function(data) {
						$scope.brokerGroups[env] = $.unique($.map(data, function(broker, index){
							return broker['group'];
						}));
						console.log($scope.brokerGroups);
					}
				}).add({
					func: TopicSync.getTopic,
					args: [env, {name: topicName}],
					success: function(result){
						$scope.views[env] = result;
						
						// If this is the view of wanted env, set current view to default one.
						if (env == $scope.env) {
							$scope.view = $scope.views[env];
						}
						
						// Trigger watcher to notify the end of one chain invocation.
						finishWatcher.step();
						
						// Abort this call chain.
						this.abort();
						$scope.$broadcast('progress', 'modalProgressBar', 'Found topic on env: ' + env);
					},
					error: function() {
						this.resume();
						$scope.$broadcast('progress', 'modalProgressBar', 'Verified topic on env: ' + env);
					}
				}).add({
					func: TopicSync.getGeneratedApplicationByType,
					args: [env, $scope.application],
					success: function(result){
						$scope.views[env] = result['value'];
						
						for (var index in result['value'].tags) {
							var tag = result['value']['tags'][index];
							$scope.selectedTags[env] = {};
							$scope.selectedTags[env][tag.group] = tag;
						}

						// Set candicate datasource for specific env.
						$scope.datasourceCandidate[env] = result['value'].partitions[0].readDatasource;
						
						// Same as above.
						if (env == $scope.env) {
							$scope.view = $scope.views[env];
						}
						$scope.$broadcast('progress', 'modalProgressBar', 'Fetched generated view on env: ' + env);
						finishWatcher.step();
					}
				}).add({
					func: TopicSync.getTags,
					args: [env],
					success: function(result) {
						$scope.tags[env] = result.data[0];
						console.log($scope.tags);
						// Keep one selection position for each tag group.
						//$scope.selectedTags = new Array(Object.keys($scope.tags).length);
					}
				}).add({
					func: TopicSync.getDatasourcesTags,
					args: [env],
					success: function(result) {
						$scope.datasourcesTags[env] = result.data[0];
						
					}
				}).finish();
				
			} else {
				var consumerName = $scope.application.productLine + '.' + $scope.application.product + '.' + $scope.application.project;
				var topicNames = $scope.application.topicName.split(',');
				remoteChain.add({
					func: TopicSync.getConsumers,
					args: [env, {topic: topicNames[0]}],
					success: function(result) {
						// Check whether specified consumer is already existed.
						var consumers = result.filter(function(elem){
							return elem['name'] == consumerName;
						});
						
						// If it's already created, abort the call chain.
						if (consumers.length > 0) {
							$scope.views[env] = result[0];
							if (env == $scope.env) {
								$scope.view = $scope.views[env];
							}
							this.abort();
							finishWatcher.step();
						}
					}
				}).add({
					func: TopicSync.getGeneratedApplicationByType,
					args: [env, $scope.application],
					success: function(result){
						$scope.views[env] = result['value'];
						if (env == $scope.env) {
							$scope.view = $scope.views[env];
						}
						$scope.$broadcast('progress', 'modalProgressBar', 'Fetched generated view on env: ' + env);
						finishWatcher.step();
					}
				}).finish();
			}
		}
		checkOnEnv('fws');
		checkOnEnv('uat');
		if ($scope.application.status > 4) {
			checkOnEnv('tools');
			checkOnEnv('prod');
		}
	};

	$scope.switchView = function($event) {
		$event.preventDefault();
		$scope.env = $($event.target).attr('href').substr(1);
		$scope.view = $scope.views[$scope.env];
		//$scope.$digest();
	};

	// Reject
	$scope.reject_application = function() {
		$scope.$broadcast('progress-random', 'modalProgressBar');
		ApplicationService.reject_application($scope.application.id, encodeComment(), user.sn).then(function(result) {
			$scope.application = result;
			decodeComment();
			$scope.$broadcast('progress-done', 'modalProgressBar');
		}, function(result) {
			$scope.$broadcast('progress-done', 'modalProgressBar');
		});
	}
	
	// Confirm user ops.
	$scope.confirm = function() {
		$('#confirmModal').modal();
	};
	
	// Ignore event trigger.
	$scope.switchView = function($event) {
		$scope.env = $($event.currentTarget).find('a').attr('href').substr(1);
		$scope.view = $scope.views[$scope.env];
	};
	
	$scope.ignore = function($event) {
		$event.preventDefault();
	}
}]);