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

			scope.is_loading = true;
			scope.src_consumers = [];
			scope.consumer_rows = [];

			scope.get_consumers = function get_consumers(table_state) {
				consumer_resource.query().$promise.then(function(query_result) {
					scope.src_consumers = query_result;
					scope.consumer_rows = filter_consumer_rows(scope.src_consumers, filter, table_state);
					scope.is_loading = false;
				});
			};

			scope.add_consumer = function add_consumer(new_consumer) {
				consumer_resource.save(new_consumer).$promise.then(function(save_result) {
					console.log(save_result);
					consumer_resource.query().$promise.then(function(query_result) {
						reload_table(scope, query_result);
						show_op_info.show("新增Consumer成功, Topic名称：" + new_consumer.name + ", Consumer名称：" + new_consumer.groupName);
					});
				});
			};

			scope.del_consumer = function del_consumer(topic, consumer) {
				bootbox.confirm("确认删除Consumer: " + consumer + "(" + topic + ")?", function(result) {
					if (result) {
						consumer_resource.remove({
							"topic" : topic,
							"consumer" : consumer
						}).$promise.then(function(remove_result) {
							consumer_resource.query().$promise.then(function(query_result) {
								reload_table(scope, query_result);
								show_op_info.show("删除Consumer：" + consumer + "(" + topic + ") 成功！");
							});
						});
					}
				});
			};
		} ]);
