var Storage = angular.module("Storage", ['ngResource'])

Storage.service("StorageService", ["$resource", function ($resource) {
    var service_value = "value from dashboard service";
    var db_size = 0;
    var db_tables = [];

    var storage_resource = $resource('/api/subscriptions/', {}, {
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
            })
        },
        delete_partition: function (datasource, table_name, callback) {
            storage_resource.delete_partition({ds: datasource, table: table_name}, function (d) {
                callback();
            })
        }
    }
}])
