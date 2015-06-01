/**
 * Created by ske on 2015/6/1.
 */
dashboard =  angular.module('hermes-dashboard',['ngResource','DashBoard'])



dashboard.controller("DashboardCtrl", function($scope, $resource, DashboardService) {
    $scope.m = "the msg from scope...";

    $scope.msg = "the msg from scope..."
    $scope.msg_from_service = (DashboardService.value)

    $scope.topics = []
    $scope.topics.push(new BuildTopic("Mock.Order.new", 222, 35, 56, "290ms", "etc"))
    $scope.topics.push(new BuildTopic("Mock.cmessage", 522, 35, 72, "15ms", "etc"))
    $scope.topics.push(new BuildTopic("Mock.Ordre.update", 222, 35, 28, "89ms", "etc"))
    $scope.topics.push(new BuildTopic("Mock.Order.1", 622, 25, 412, "99ms", "etc"))
    $scope.topics.push(new BuildTopic("Mock.Order.2", 122, 92, 123, "1000ms", "etc"))
    $scope.topics.push(new BuildTopic("Mock.Order.3", 122, 35, 63, "290ms", "etc"))

})


var BuildTopic = function(name, qps, j, io, latency, etc) {
    var topic = {}
    topic.name = name;
    topic.qps = qps;
    topic.j = j;
    topic.io = io;
    topic.latency = latency;
    topic.etc = etc;
    return topic;
}
