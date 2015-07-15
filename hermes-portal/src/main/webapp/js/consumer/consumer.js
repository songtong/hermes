function filter_consumer_rows(rows, filter, table_state) {
	rows = table_state.search.predicateObject ? filter('filter')(rows, table_state.search.predicateObject) : rows;
	if (table_state.sort.predicate) {
		rows = filter('orderBy')(rows, table_state.sort.predicate, table_state.sort.reverse);
	}
	return rows;
}

function reload_table(scope, data) {
	scope.src_consumers = data;
	scope.consumer_rows = scope.src_consumers;
}

angular.module('hermes-consumer', [ 'ngResource', 'smart-table' ]).controller('consumer-controller',
		[ '$scope', '$filter', '$resource', function(scope, filter, resource) {
			consumer_resource = resource('/api/consumers/:topic/:consumer', {}, {});
			meta_resource = resource('/api/meta', {}, {
				'get_topic_names' : {
					method : 'GET',
					isArray : true,
					url : '/api/meta/topics/names'
				}
			})

			scope.is_loading = true;
			scope.src_consumers = [];
			scope.consumer_rows = [];
			scope.new_consumer = {
				orderedConsume : true
			};

			scope.order_opts = [ true, false ];

			meta_resource.get_topic_names({}, function(result) {
				$('#inputTopicName').typeahead({
					name : 'topics',
					source : substringMatcher(result)
				});
			});

			scope.get_consumers = function get_consumers(table_state) {
				consumer_resource.query().$promise.then(function(query_result) {
					scope.src_consumers = query_result;
					scope.consumer_rows = filter_consumer_rows(scope.src_consumers, filter, table_state);
					scope.is_loading = false;
				});
			};

			scope.add_consumer = function add_consumer(new_consumer) {
				consumer_resource.save({
					topic : new_consumer.topic,
					consumer : new_consumer.groupName
				}, new_consumer, function(save_result) {
					console.log(save_result);
					consumer_resource.query().$promise.then(function(query_result) {
						reload_table(scope, query_result);
						show_op_info.show("新增成功: " + new_consumer.groupName + "(" + new_consumer.topic + ")", true);
					});
				}, function(error_result) {
					show_op_info.show("新增失败: " + new_consumer.groupName + "(" + new_consumer.topic + "), " + error_result.data, false);
				});
			};

			scope.del_consumer = function del_consumer(topic, consumer) {
				bootbox.confirm("确认删除 Consumer: " + consumer + "(" + topic + ")?", function(result) {
					if (result) {
						consumer_resource.remove({
							"topic" : topic,
							"consumer" : consumer
						}, function(remove_result) {
							consumer_resource.query({}, function(query_result) {
								reload_table(scope, query_result);
								show_op_info.show("删除成功: " + consumer + "(" + topic + ")", true);
							});
						}, function(error_result) {
							show_op_info.show("删除失败: " + consumer + "(" + topic + "), " + error_result.data, false);
						});
					}
				});
			};
		} ]);
