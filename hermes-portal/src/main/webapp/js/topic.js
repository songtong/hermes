'use strict'

function to_topic_rows(topics) {
	var rows = [];
	for (var i = 0; i < topics.length; i++) {
		var row = angular.copy(topics[i]);
		row.partitions = row.partitions.length;
		rows.push(row);
	}
	return rows;
}

function filter_topic_rows(rows, filter, table_state) {
	rows = table_state.search.predicateObject ? filter('filter')(rows, table_state.search.predicateObject) : rows;
	if (table_state.sort.predicate) {
		rows = filter('orderBy')(rows, table_state.sort.predicate, table_state.sort.reverse);
	}
	return rows;
}

function find_datasource_names($scope, type) {
	for (var i = 0; i < $scope.src_storages.length; i++) {
		if ($scope.src_storages[i].type == type) {
			return collect_schemas($scope.src_storages[i].datasources, 'id', false);
		}
	}
}

function find_endpoint_names($scope, type) {
	names = [];
	for (var i = 0; i < $scope.src_endpoints.length; i++) {
		if ($scope.src_endpoints[i].type == type) {
			names.push($scope.src_endpoints[i].id);
		}
	}
	return names;
}

function reload_table($scope, data) {
	$scope.src_topics = data;
	$scope.topic_rows = to_topic_rows($scope.src_topics);
}

function to_required_topic(data) {
	var topic = {};
	topic.name = data.name;
	topic.codecType = data.codecType;
	topic.storageType = data.storageType;
	topic.endpointType = data.endpointType;
	topic.ackTimeoutSeconds = data.ackTimeoutSeconds;
	topic.consumeRetryPolicy = data.consumeRetryPolicy;

	topic.partitions = [];
	topic.partitions.push(data.partition);

	return topic;
}

angular.module('hermes-topic', [ 'ngResource', 'ui.bootstrap', 'smart-table' ]).controller('topic-controller',
		[ '$scope', '$filter', '$resource', function($scope, filter, resource) {
			var topic_resource = resource('/api/topics/:name', {}, {});
			var meta_resource = resource('/api/meta/', {}, {
				'get_codecs' : {
					method : 'GET',
					isArray : true,
					url : '/api/meta/codecs'
				},
				'get_storages' : {
					method : 'GET',
					isArray : true,
					url : '/api/meta/storages'
				},
				'get_endpoints' : {
					method : 'GET',
					isArray : true,
					url : '/api/meta/endpoints'
				}
			});

			$scope.cur_time = new Date();

			$scope.is_loading = true;
			$scope.src_topics = [];
			$scope.topic_rows = [];

			$scope.new_topic = {
				partitions : [ {} ],
				consumerRetryPolicy : '1:[3,6,9]',
				ackTimeoutSeconds : 5,
				endpointType : 'broker',
				storageType : 'mysql',
				codecType : 'json'
			};

			$scope.codec_types = [ 'json', 'avro', 'cmessaging' ];
			$scope.endpoint_types = [ 'broker', 'kafka' ];
			$scope.storage_types = [ 'mysql', 'kafka' ];

			meta_resource.get_endpoints({}, function(data) {
				$scope.endpoint_names = {
					'broker' : [],
					'kafka' : []
				};
				for (var i = 0; i < data.length; i++) {
					$scope.endpoint_names[data[i].type].push(data[i].id);
				}
			});

			meta_resource.get_storages({}, function(data) {
				$scope.datasource_names = {
					'mysql' : [],
					'kafka' : []
				};
				for (var i = 0; i < data.length; i++) {
					for (var j = 0; j < data[i].datasources.length; j++) {
						$scope.datasource_names[data[i].type].push(data[i].datasources[j].id);
					}
				}
			});

			$scope.$watchGroup([ 'new_topic.endpointType', 'endpoint_names' ], function() {
				if ($scope.endpoint_names !== undefined) {
					update_new_partitions_endpoint();
				}
			});

			$scope.$watchGroup([ 'new_topic.storageType', 'datasource_names' ], function() {
				if ($scope.datasource_names !== undefined) {
					update_new_partitions_datasource();
				}
			});

			function update_new_partitions_endpoint() {
				for (var i = 0; i < $scope.new_topic.partitions.length; i++) {
					$scope.new_topic.partitions[i].endpoint = $scope.endpoint_names[$scope.new_topic.endpointType][0];
				}
			}

			function update_new_partitions_datasource() {
				for (var i = 0; i < $scope.new_topic.partitions.length; i++) {
					$scope.new_topic.partitions[i].readDatasource = $scope.datasource_names[$scope.new_topic.storageType][0];
					$scope.new_topic.partitions[i].writeDatasource = $scope.datasource_names[$scope.new_topic.storageType][0];
				}
			}

			$scope.get_topics = function get_topics(table_state) {
				topic_resource.query({}, function(query_result) {
					$scope.src_topics = query_result;
					$scope.topic_rows = filter_topic_rows(to_topic_rows($scope.src_topics), filter, table_state);
					$scope.is_loading = false;
				});
			};

			$scope.add_topic = function add_topic(new_topic) {
				topic_resource.save(new_topic, function(save_result) {
					console.log(save_result);
					topic_resource.query({}, function(query_result) {
						reload_table($scope, query_result);
						show_op_info.show("新增 " + new_topic.name + " 成功！", true);
					});
				}, function(error_result) {
					show_op_info.show("新增 " + new_topic.name + " 失败: " + error_result.data, false);
				});
			};

			$scope.add_new_partition = function add_new_partition() {
				var new_partition = {};
				new_partition.readDatasource = $scope.datasource_names[$scope.new_topic.storageType][0];
				new_partition.writeDatasource = $scope.datasource_names[$scope.new_topic.storageType][0];
				new_partition.endpoint = $scope.endpoint_names[$scope.new_topic.endpointType][0];
				$scope.new_topic.partitions.push(new_partition);
			}

			$scope.del_one_partition = function del_one_partition() {
				$scope.new_topic.partitions.splice($scope.new_topic.partitions.length - 1, 1);
			}

			$scope.del_topic = function del_topic(name) {
				bootbox.confirm("确认删除: " + name + " ?", function(result) {
					if (result) {
						topic_resource.remove({
							"name" : name
						}, function(remove_result) {
							topic_resource.query({}, function(query_result) {
								reload_table($scope, query_result);
								show_op_info.show("删除 " + name + " 成功！", true);
							});
						}, function(error_result) {
							show_op_info.show("删除 " + name + " 失败: " + error_result.data, false);
						});
					}
				});
			};
		} ]);