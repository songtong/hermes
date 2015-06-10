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

angular.module('hermes-storage', [ 'ngResource', 'xeditable' ]).run(function(editableOptions) {
	editableOptions.theme = 'bs3';
}).controller('storage-controller', [ '$scope', '$resource', function(scope, resource) {
	var meta_resource = resource('/api/meta/', {}, {
		'get_storages' : {
			method : 'GET',
			isArray : true,
			url : '/api/meta/storages'
		},
		'update_datasource' : {
			method : 'POST',
			url : '/api/meta/storages/:type/:id/update'
		},
		'delete_property' : {
			method : 'DELETE',
			url : '/api/meta/storages/:type/:id/delprop'
		}
	});

	scope.src_storages = [];
	scope.selected = {};

	meta_resource.get_storages({}, function(data) {
		scope.src_storages = data;
		scope.storage_types = collect_schemas(scope.src_storages, 'type', false);
		scope.selected = scope.src_storages[0];
	});

	scope.set_selected = function set_selected(type) {
		for (var idx = 0; idx < scope.src_storages.length; idx++) {
			if (scope.src_storages[idx].type == type) {
				scope.selected = scope.src_storages[idx];
				break;
			}
		}
	}

	scope.update_datasource = function update_datasource(ds) {
		meta_resource.update_datasource({
			'type' : scope.selected.type,
			'id' : ds.id
		}, ds, function(result) {
			show_op_info.show("更新Datasource：" + ds.id + " 成功！");
		});
	}

	scope.add_row = function add_row(ds) {
		scope.inserted = {
			name : undefined,
			value : undefined
		};
		ds.properties['_hermes_new_row'] = scope.inserted;
	}

	scope.del_row = function del_row(ds, name) {
		bootbox.confirm("确认删除属性: " + ds.id + "(" + name + ")?", function(result) {
			if (result) {
				meta_resource.delete_property({
					'type' : scope.selected.type,
					'id' : ds.id,
					'name' : name
				}, function(remove_result) {
					show_op_info.show("删除属性: " + ds.id + "(" + name + ") 成功！");
				});
				delete ds.properties[name];
			}
		});
	}
} ]);
