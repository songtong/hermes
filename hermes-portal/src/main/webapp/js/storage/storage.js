var hermes_storage = angular.module('hermes-storage', [ 'ngResource', 'xeditable', 'mgcrea.ngStrap','Storage', 'smart-table', 'components' ]);
hermes_storage.run(function(editableOptions) {
	editableOptions.theme = 'bs3';
}).controller('storage-controller', [ '$scope', '$resource', 'StorageService', function(scope, resource, StorageService) {
	// Define resource.
	var meta_resource = resource('/api/storages', {}, {
		'get_storages' : {
			method : 'GET',
			isArray : true
		}
	});
	
	// All datasources.
	scope.__datasources = null;
	// Selected datasources.
	scope.datasources = null;
	
	scope.currentDatasource = null;
	
	//
	scope.storageType = 'kafka';

	// Init.
    (function() {
        meta_resource.get_storages({}, function (data) {
            scope.__datasources = data;
            scope.datasources = data[0];
            scope.selectStorage(scope.storageType);
        });
    })();
    
    scope.selectStorage = function(type) {
    	scope.storageType = type;
    	if (type == 'mysql') {
    		scope.datasources = scope.__datasources[1].datasources;
    	} else {
    		scope.datasources = scope.__datasources[0].datasources;
    	}
    }
    
    scope.edit = function(index) {
    	scope.currentDatasource = scope.datasources[index];
    	$('#datasource-modal').modal();
    };
    
    scope.reset = function($event) {
    	$($event.currentTarget).parents('.modal-footer').siblings('.modal-body').find('form input').val('');
    }
    
    scope.add = function() {
    	scope.currentDatasource = {};
    }
    
    scope.save = function() {
    	StorageService.add_datasource(scope.currentDatasource, scope.storageType, function(data){
    		scope.currentDatasource = data;
    		scope.datasources.push(scope.currentDatasource);
    	});
    }
    
    scope.remove = function(context) {
    	StorageService.delete_datasource(scope.datasources[context.index].id, scope.storageType, function(){
        	scope.datasources.splice(context.index, 1);
        	// Angular bug: can not handle collection changes when using ng-repeat in template.
        	$(context.target).parents('tr').remove();
    	});
    }
    
    scope.confirm = function($index, $event) {
    	scope.$broadcast('confirm', 'confirmDialog', {index: $index, target: $event.currentTarget});
    };

    scope.set_selected = function set_selected(type) {
		for (var idx = 0; idx < scope.src_storages.length; idx++) {
			if (scope.src_storages[idx].type == type) {
				scope.selected = scope.src_storages[idx];
				break;
			}
		}
	}

	scope.update_datasource = function update_datasource(ds) {
		StorageService.update_datasource(scope.selected.type, ds.id,ds);
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
				StorageService.delete_property(scope.selected.type,ds.id,name);
				delete ds.properties[name];
			}
		});
	}

    scope.is_mysql = function (type) {
        return type == 'mysql';
    }

//    scope.$watch(function() {return scope.selected.type}, function() {
//        if (scope.selected.type != undefined) {
//            scope.forms = buildForms(scope.selected.type);
//        }
//    })

    scope.add_kv = function() {
        scope.forms.push(buildForm("", "", ""))
    }

    scope.del_kv = function(index) {
        scope.forms.splice(index, 1);
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


