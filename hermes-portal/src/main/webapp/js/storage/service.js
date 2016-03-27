var Storage = angular.module("Storage", ['ngResource'])

Storage.service("StorageService", ["$resource", function ($resource) {
    var service_value = "value from dashboard service";
    var db_size = 0;
    var db_tables = [];

    var storage_resource = $resource('/api/', {}, {
        get_size: {
            method: 'GET',
            isArray: false,
            url: '/api/storage/size'
        },
        get_tables: {
            method: 'GET',
            isArray: true,
            url: '/api/storage/tables'
        },
        get_partitions: {
            method: 'GET',
            isArray: true,
            url: '/api/storage/partitions'
        },
        add_partition: {
            method: 'GET',
            isArray: false,
            url: '/api/storage/addp'
        },
        delete_partition: {
            method: 'GET',
            isArray: false,
            url: '/api/storage/deletep'
        },
        add_datasource: {
            method: 'POST',
            isArray: false,
            url: '/api/datasources/:type'
        },
        delete_datasource: {
            method: 'DELETE',
            isArray: false,
            url: '/api/datasources/:type/:id'
        },
       	update_datasource : {
			method : 'POST',
			url : '/api/datasources/:type/:id/update'
		},
		delete_property : {
			method : 'DELETE',
			url : '/api/datasources/:type/:id/delprop'
		}
    });


    function filterTable(storageTables) {
        for (var i = 0; i < storageTables.length; i++) {
            // 1. set table.size
            storageTables[i].size = sizeToString(storageTables[i].dataLength + storageTables[i].indexLength);

            // 2. set table.span by table.name
            var name = storageTables[i].name;
            if (name.indexOf("message") != -1 &&
            name.indexOf("offset_message") == -1 ) {
                storageTables[i].span = 100 * 10000;
            } else if (name.indexOf("resend") != -1 &&
                name.indexOf("offset_resend") == -1 ) {
                storageTables[i].span = 10 * 10000;
            } else {
                storageTables[i].span = 1 * 10000;
            }
        }
        return storageTables;
    }

    function filterPartition(pa) {
        for (var i = 0; i < pa.length; i++) {
            // 1. set partition.size
            pa[i].size = sizeToString(pa[i].dataLength + pa[i].indexLength);

            // 2. set partition.nameOrder for ordering by partition.name
            var nameIndex = pa[i].name.substring(1);
            if (parseInt(nameIndex) >= 0) {
                pa[i].nameOrder = parseInt(nameIndex);
            } else {
                pa[i].nameOrder = Infinity;
            }
        }
        return pa;
    }

    function sizeToString(size) {
        if (size > 1024 * 1024 * 1024) {
            return (size / (1024 * 1024 * 1024)).toFixed(2) + "G";
        } else if (size > 1024 * 1024) {
            return (size / (1024 * 1024)).toFixed(2) + "M";
        } else if (size > 1024) {
            return (size / 1024).toFixed(2) + "K";
        } else {
            return size + "B";
        }
    }

    return {
        value: service_value,
        update_datasource : function(type,id,ds) {
     		storage_resource.update_datasource({
				'type' : type,
				'id' : id
				}, ds, function(result) {
					show_op_info.show("更新Datasource：" + ds.id + " 成功！",true);
			});
		},
		delete_property : function(type,id,name){
			storage_resource.delete_property({
					'type' : type,
					'id' : id,
					'name' : name
				}, function(remove_result) {
					show_op_info.show("删除属性: " + ds.id + "(" + name + ") 成功！");
				});
		},
        get_db_size: function () {
            return sizeToString(db_size);
        },

        get_db_tables: function () {
            return db_tables;
        },

        updateDBSize: function (datasource) {
            storage_resource.get_size({ds: datasource}, function (d) {
                db_size = d.size;
            });
        },

        updateTables: function (datasource) {
            storage_resource.get_tables({ds: datasource}, function (d) {
                db_tables = filterTable(d);
            })
        },

        updatePartitionsIntoTable: function (datasource, table) {
            storage_resource.get_partitions({ds: datasource, table: table.name}, function (d) {
                table.partitions = filterPartition(d);
            })
        },

        add_partition: function (datasource, table_name, span, callback) {
            storage_resource.add_partition({ds: datasource, table: table_name, span: span}, function (d) {
                callback();
                show_op_info.show("新增Partition成功", true);
            }, function(error) {
                show_op_info.show("新增Partition失败", false);
            })
        },
        delete_partition: function (datasource, table_name, callback) {
            storage_resource.delete_partition({ds: datasource, table: table_name}, function (d) {
                callback();
                show_op_info.show("删除Partition成功", true);
            }, function(error) {
                show_op_info.show("删除Partition失败", false);
            })
        },
        add_datasource: function(datasource, type, callback) {
            storage_resource.add_datasource({"type": type}, JSON.stringify(datasource), function(d){
                callback(d);
                show_op_info.show("增加Datasource成功", true);
            }, function(error) {
                show_op_info.show("删除Datasource失败", false);
            })
        },
        delete_datasource: function(id, type, callback) {
            storage_resource.delete_datasource({"id": id, "type": type}, function(d) {
                callback(d);
                show_op_info.show("删除Datasource成功", true);
            }, function(error) {
                show_op_info.show("删除Datasource失败", false);
            })
        }
    }
}])

function buildDatasource(forms) {
    var ds = {};
    var properties = {}

    // don't ask why building Datasource so ugly, this is what Datasource is like.
    for(var i = 0; i < forms.length; i++) {
        var key = forms[i].key;
        var value = forms[i].value;
        if (key == "id") {
            ds.id = value;
        } else {
            var object = {};
            object.name = key;
            object.value = value;
            properties[key] = object;
        }
    }
    ds.properties = properties;
    return ds;
}
