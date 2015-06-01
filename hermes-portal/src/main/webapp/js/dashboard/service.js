/**
 * Created by ske on 2015/6/1.
 */
Dashboard = angular.module("DashBoard", [])

Dashboard.service("DashboardService", ["$resource", function ($resoource) {
    var service_value = "value fro dashboard service";


    return {
        value: service_value
    }
}])