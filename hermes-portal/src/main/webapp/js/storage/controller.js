hermes_storage.directive("storage", function () {
    return {
        restrict: 'EA',
        templateUrl: '../jsp/console/storage/mysql-storage.html',
        replace: true,
        controller: 'MysqlCtrl',
        scope: {storage_id: "=id", storage_type: "=type"}
    };
});

hermes_storage.controller('MysqlCtrl', function ($scope, $resource, $log, $attrs, StorageService) {

    // set show threshold
    $scope.showThreshold = 1000;

    // update and watch db_size and tables.
    StorageService.updateDBSize($scope.storage_id)
    StorageService.updateTables($scope.storage_id)
    $scope.$watch(StorageService.get_db_size, function (newValue, oldValue) {
        $scope.db_size = StorageService.get_db_size();
    })

    $scope.$watch(StorageService.get_db_tables, function () {
        $scope.tables = StorageService.get_db_tables();
    })


    // add_partition and delete_partition
    $scope.add_pa = function (table) {
        table.isShowPas = false;

        if (validateInput(table.span)) {
            StorageService.add_partition($scope.storage_id, table.name, table.span, function () {
                StorageService.updateDBSize($scope.storage_id);
                StorageService.updateTables($scope.storage_id);
                StorageService.updatePartitionsIntoTable($scope.storage_id, table);
            })
        }
    }

    $scope.delete_pa = function (table) {
        StorageService.delete_partition($scope.storage_id, table.name, function() {
            StorageService.updateDBSize($scope.storage_id);
            StorageService.updateTables($scope.storage_id);
            StorageService.updatePartitionsIntoTable($scope.storage_id, table);
        })
    }

    $scope.clickOnTable = function (table) {
        if (table.isShowPas == undefined) {
            table.isShowPas = true;
            StorageService.updatePartitionsIntoTable($scope.storage_id, table);
        } else {
            table.isShowPas = !table.isShowPas;
            if (table.isShowPas) {
                StorageService.updatePartitionsIntoTable($scope.storage_id, table);
            }
        }
    }


    // for show main tables or show all tables
    $scope.isShowAllTables = false;

    $scope.briefOrAll = function (table) {
        if ($scope.isShowAllTables) {
            return true
        } else {
            var tableNameFilter = table.name.indexOf("dead_letter") != -1 ||
                (table.name.indexOf("message") != -1 &&
                table.name.indexOf("offset_message") == -1 ) ||
                ( table.name.indexOf("resend") != -1 &&
                table.name.indexOf("offset_resend") == -1 );
            var tableRowsFilter = table.tableRows > $scope.showThreshold
            return tableNameFilter && tableRowsFilter
        }
    }

    // for debug:
    $scope.show = function () {
        alert($scope.isShowAllTables)
    }

});

function validateInput(span) {
    if (parseInt(span) >=0 && span >= 10000) {
        return true;
    } else {
        alert("Please Input Write Number (min is 10000)! Your Input is " + span);
        return false;
    }
}
