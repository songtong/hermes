function filter_endpoint_rows(rows, filter, table_state) {
	rows = table_state.search.predicateObject ? filter('filter')(rows, table_state.search.predicateObject) : rows;
	if (table_state.sort.predicate) {
		rows = filter('orderBy')(rows, table_state.sort.predicate, table_state.sort.reverse);
	}
	return rows;
}

function reload_table(scope, data) {
	scope.src_endpoints = data;
	scope.endpoint_rows = scope.src_endpoints;
}

angular.module('hermes-endpoint', [ 'ngResource', 'smart-table' ]).controller('endpoint-controller',
		[ '$scope', '$filter', '$resource', function(scope, filter, resource) {
			var meta_resource = resource('/api/endpoints', {}, {
				'get_endpoints' : {
					method : 'GET',
					isArray : true,
				},
				'add_endpoint' : {
					method : 'POST',
				},
				'delete_endpoint' : {
					method : 'DELETE',
					url : '/api/endpoints/:id'
				}
			});

			scope.is_loading = true;
			scope.src_endpoints = [];
			scope.endpoint_rows = [];
			scope.new_endpoint = {
				host : '127.0.0.1',
				port : 4376
			};

			scope.get_endpoints = function get_endpoints(table_state) {
				meta_resource.get_endpoints({}).$promise.then(function(query_result) {
					scope.src_endpoints = query_result;
					scope.endpoint_rows = filter_endpoint_rows(scope.src_endpoints, filter, table_state);
					scope.is_loading = false;
					$('#inputEndpointType').typeahead({
						name : 'topics',
						source : substringMatcher(collect_schemas(query_result, 'type', true))
					});
				});
			};

			scope.add_endpoint = function add_endpoint(new_endpoint) {
				meta_resource.add_endpoint({}, new_endpoint).$promise.then(function(save_result) {
					console.log(save_result);
					meta_resource.get_endpoints({}, function(query_result) {
						reload_table(scope, query_result);
						show_op_info.show("新增成功, 名称: " + new_endpoint.id, true);
					}, function(error_result) {
						show_op_info.show("新增失败: " + error_result.data, false);
					});
				});
			};

			scope.del_endpoint = function del_endpoint(id) {
				bootbox.confirm("确认删除 Endpoint: " + id + "?", function(result) {
					if (result) {
						meta_resource.delete_endpoint({
							"id" : id
						}, function(remove_result) {
							meta_resource.get_endpoints().$promise.then(function(query_result) {
								reload_table(scope, query_result);
								show_op_info.show("删除成功: " + id, true);
							});
						}, function(error_result) {
							show_op_info.show("删除失败: " + error_result.data, false);
						});
					}
				});
			};
		} ]);
