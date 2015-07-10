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

function find_datasource_names(scope, type) {
	for (var i = 0; i < scope.src_storages.length; i++) {
		if (scope.src_storages[i].type == type) {
			return collect_schemas(scope.src_storages[i].datasources, 'id', false);
		}
	}
}

function find_endpoint_names(scope, type) {
	names = [];
	for (var i = 0; i < scope.src_endpoints.length; i++) {
		if (scope.src_endpoints[i].type == type) {
			names.push(scope.src_endpoints[i].id);
		}
	}
	return names;
}

function reload_table(scope, data) {
	scope.src_topics = data;
	scope.topic_rows = to_topic_rows(scope.src_topics);
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
		[ '$scope', '$filter', '$resource', function(scope, filter, resource) {
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

			scope.cur_time = new Date();

			scope.is_loading = true;
			scope.src_topics = [];
			scope.topic_rows = [];

			scope.new_topic = {
				partitions : [ {} ],
				consumerRetryPolicy : '1:[3,6,9]',
				ackTimeoutSeconds : 5
			};

			scope.codec_types = meta_resource.get_codecs({}, function(data) {
				scope.codec_types = collect_schemas(data, 'type', true);
				scope.new_topic.codecType = scope.codec_types[0];
			});

			scope.storage_types = meta_resource.get_storages({}, function(data) {
				scope.src_storages = data;
				scope.storage_types = collect_schemas(scope.src_storages, 'type', true);
				scope.new_topic.storageType = scope.storage_types[0];
				scope.datasource_names = find_datasource_names(scope, scope.new_topic.storageType);
				scope.new_topic.partitions[0].readDatasource = scope.datasource_names[0];
				scope.new_topic.partitions[0].writeDatasource = scope.datasource_names[0];
			});

			scope.endpoint_types = meta_resource.get_endpoints({}, function(data) {
				scope.src_endpoints = data;
				scope.endpoint_types = unique_array(collect_schemas(data, 'type', false));
				scope.new_topic.endpointType = scope.endpoint_types[0];
				scope.endpoint_names = find_endpoint_names(scope, scope.new_topic.endpointType);
				scope.new_topic.partitions[0].endpoint = scope.new_topic.storageType == "kafka" ? scope.endpoint_names[0] : null;
			});

			scope.get_topics = function get_topics(table_state) {
				topic_resource.query({}, function(query_result) {
					scope.src_topics = query_result;
					scope.topic_rows = filter_topic_rows(to_topic_rows(scope.src_topics), filter, table_state);
					scope.is_loading = false;
				});
			};

			scope.add_topic = function add_topic(new_topic) {
				topic_resource.save(new_topic, function(save_result) {
					console.log(save_result);
					topic_resource.query({}, function(query_result) {
						reload_table(scope, query_result);
						show_op_info.show("新增 " + new_topic.name + " 成功！", true);
					});
				}, function(error_result) {
					show_op_info.show("新增 " + new_topic.name + " 失败: " + error_result.data, false);
				});
			};
			
			scope.show_something = function show_something(){
				console.log('fuckoff');
			}

			scope.add_new_partition = function add_new_partition() {
				var new_partition = {};
				new_partition.readDatasource = scope.datasource_names[0];
				new_partition.writeDatasource = scope.datasource_names[0];
				new_partition.endpoint = scope.new_topic.storageType == "kafka" ? scope.endpoint_names[0] : null;
				scope.new_topic.partitions.push(new_partition);
			}
			
			scope.del_one_partition = function del_one_partition() {
				scope.new_topic.partitions.splice(scope.new_topic.partitions.length - 1, 1);
			}

			scope.del_topic = function del_topic(name) {
				bootbox.confirm("确认删除: " + name + " ?", function(result) {
					if (result) {
						topic_resource.remove({
							"name" : name
						}, function(remove_result) {
							topic_resource.query({}, function(query_result) {
								reload_table(scope, query_result);
								show_op_info.show("删除 " + name + " 成功！", true);
							});
						}, function(error_result) {
							show_op_info.show("删除 " + name + " 失败: " + error_result.data, false);
						});
					}
				});
			};

			scope.storage_type_changed = function storage_type_changed() {
				scope.datasource_names = find_datasource_names(scope, scope.new_topic.storageType);
				for (var i = 0; i < scope.new_topic.partitions.length; i++) {
					scope.new_topic.partitions[i].readDatasource = scope.datasource_names[0];
					scope.new_topic.partitions[i].writeDatasource = scope.datasource_names[0];
					scope.new_topic.partitions[i].endpoint = scope.new_topic.storageType == "kafka" ? scope.endpoint_names[0] : null;
				}
			}

			scope.endpoint_type_changed = function endpoint_type_changed() {
				scope.endpoint_names = find_endpoint_names(scope, scope.new_topic.endpointType);
				for (var i = 0; i < scope.new_topic.partitions.length; i++) {
					scope.new_topic.partitions[i].endpoint = scope.new_topic.storageType == "kafka" ? scope.endpoint_names[0] : null;
				}
			}
		} ]);