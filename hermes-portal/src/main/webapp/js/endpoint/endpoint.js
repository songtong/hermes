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

angular.module('hermes-endpoint', [ 'ngResource', 'smart-table', 'xeditable' ]).run(function(editableOptions) {
	editableOptions.theme = 'bs3';
}).controller('endpoint-controller', [ '$scope', '$filter', '$resource', function(scope, filter, resource) {
	var endpoint_resource = resource('/api/endpoints', {}, {
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
		},
		'update_endpoint' : {
			method : 'POST',
			url : '/api/endpoints/update'
		},
		'get_broker_groups' : {
			method : 'GET',
			url : '/api/endpoints/brokerGroups',
			isArray : true
		}
	});

	scope.is_loading = true;
	scope.src_endpoints = [];
	scope.endpoint_rows = [];
	scope.broker_groups = [];
	scope.types = [ 'broker', 'kafka' ];
	scope.new_endpoint = {
		host : '127.0.0.1',
		type : 'broker',
		port : 4376,
		group : 'default'
	};

	scope.get_endpoints = function get_endpoints(table_state) {
		endpoint_resource.get_endpoints({}).$promise.then(function(query_result) {
			scope.src_endpoints = query_result;
			scope.endpoint_rows = filter_endpoint_rows(scope.src_endpoints, filter, table_state);
			scope.is_loading = false;
			$('#inputEndpointType').typeahead({
				name : 'topics',
				source : substringMatcher(collect_schemas(query_result, 'type', true))
			});
		});
	};

	scope.get_broker_groups = function get_broker_groups() {
		endpoint_resource.get_broker_groups().$promise.then(function(result) {

			var result = new Bloodhound({
				local : result,
				datumTokenizer : Bloodhound.tokenizers.whitespace,
				queryTokenizer : Bloodhound.tokenizers.whitespace,
			});
			$('#inputEndpointGroup').typeahead({
				  minLength: 1,
				  highlight: true
				},
				{
				  name: 'broker_groups',
				  source: result
				});
		
		})
	}
	console.log(scope.get_broker_groups());
	scope.broker_groups = scope.get_broker_groups();
	
	

	scope.display_broker_groups = [].concat(scope.broker_groups);

	scope.add_endpoint = function add_endpoint(new_endpoint) {
		endpoint_resource.add_endpoint({}, new_endpoint).$promise.then(function(save_result) {
			console.log(save_result);
			endpoint_resource.get_endpoints({}, function(query_result) {
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
				endpoint_resource.delete_endpoint({
					"id" : id
				}, function(remove_result) {
					endpoint_resource.get_endpoints().$promise.then(function(query_result) {
						reload_table(scope, query_result);
						show_op_info.show("删除成功: " + id, true);
					});
					scope.get_broker_groups();
				}, function(error_result) {
					show_op_info.show("删除失败: " + error_result.data, false);
				});
			}
		});
	};

	scope.update_endpoint = function(row) {
		console.log(row)
		bootbox.confirm("确认保存 Endpoint: " + row.id + "?", function(result) {
			if (result) {
				endpoint_resource.update_endpoint(row, function(result) {
					show_op_info.show("更新成功" , true);
					endpoint_resource.get_endpoints().$promise.then(function(query_result) {
						reload_table(scope, query_result);
					});
					scope.get_broker_groups();
				}, function(error_result) {
					show_op_info.show("更新失败: " + error_result.data, false);
				});
			}
		});
	}
} ]);
