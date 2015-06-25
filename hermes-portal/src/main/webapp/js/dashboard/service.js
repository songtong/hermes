Dashboard = angular.module("DashBoard", [])

Dashboard.service("DashboardService", ["$resource", function ($resoource) {
    var service_value = "value fro dashboard service";


    return {
        value: service_value
    }
}])