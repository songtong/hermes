var hermes_storage = angular.module('hermes-storage', [ 'ngResource', 'xeditable', 'mgcrea.ngStrap','Storage']);
hermes_storage.run(function(editableOptions) {
	editableOptions.theme = 'bs3';
}).controller('storage-controller', [ '$scope', '$resource', 'StorageService', function(scope, resource, StorageService) {
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

    function getDatasources() {
        meta_resource.get_storages({}, function (data) {
            scope.src_storages = data;
            scope.storage_types = collect_schemas(scope.src_storages, 'type', false);
            scope.selected = scope.src_storages[0];
        });
    }
    getDatasources();

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

    scope.is_mysql = function (type) {
        return type == 'mysql';
    }

    scope.$watch(function() {return scope.selected.type}, function() {
        if (scope.selected.type != undefined) {
            scope.forms = buildForms(scope.selected.type);
        }
    })

    scope.add_kv = function() {
        scope.forms.push(buildForm("", "", ""))
    }

    scope.del_kv = function(index) {
        scope.forms.splice(index, 1);
    }

    scope.reset = function() {
        scope.forms = buildForms(scope.selected.type);
    }

    scope.add_datasource = function() {
        StorageService.add_datasource(scope.forms, scope.selected.type, getDatasources);
    }

    scope.del_datasource = function(ds) {
        bootbox.confirm("确认删除该Datasource? ", function(result) {
            if (result) {
                StorageService.delete_datasource(ds.id, scope.selected.type, getDatasources)
            }
        })
    }
    scope.isShowAllTables = false;
    scope.isShowAll= function() {
        scope.isShowAllTables = !scope.isShowAllTables;
    };
} ]);

function buildForms(dsType) {
    var forms = [];
        if (dsType.toLowerCase() == "kafka") {
            forms.push(buildForm("id", "", "id"))
            forms.push(buildForm("bootstrap.servers", "", "xxxx:9092"))
            forms.push(buildForm("offsets.storage", "kafka", ""))
            forms.push(buildForm("zookeeper.connect", "", "xxxx:2181"))
        } else {
            forms.push(buildForm("id", "", "id"))
            forms.push(buildForm("url", "", "jdbc:mysql://..."))
            forms.push(buildForm("user", "", "user"))
            forms.push(buildForm("password", "", "password"))
        }
        return forms
}

function buildForm(key, value, placeholder) {
    var form = {};
    form.key = key;
    form.value = value;
    form.placeholder = placeholder;
    return form;
}


