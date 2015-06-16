/**
 * Created by ske on 2015/6/1.
 */
dashboard = angular.module('hermes-dashboard', ['ngResource', 'DashBoard'])


dashboard.controller("DashboardCtrl", function ($scope, $resource, DashboardService) {
    $scope.m = "the msg from scope...";

    $scope.msg = "the msg from scope..."
    $scope.msg_from_service = (DashboardService.value)

    $scope.topics = []
    $scope.topics.push(new BuildTopic("Order.new", 105, 35, 1087, "77ms", "etc"))
    $scope.topics.push(new BuildTopic("cmessage", 861, 92, 2051, "15ms", "etc"))
    $scope.topics.push(new BuildTopic("Ordre.update", 20, 35, 201, "89ms", "etc"))
    $scope.topics.push(new BuildTopic("Order.1", 12, 5, 11, "99ms", "etc"))
    $scope.topics.push(new BuildTopic("Order.2", 8, 8, 4, "10ms", "etc"))
    $scope.topics.push(new BuildTopic("Order.3", 92, 11, 382, "29ms", "etc"))

    $scope.consumers = []
    $scope.consumers.push(new BuildConsumer("group1", "cmessage_consumer1", "cmessage", 435, "47ms"))
    $scope.consumers.push(new BuildConsumer("group1", "cmessage_consumer2", "cmessage", 421, "85ms"))
    $scope.consumers.push(new BuildConsumer("group2", "cmessage_consumer1", "cmessage", 856, "73ms"))
    $scope.consumers.push(new BuildConsumer("MyGroup", "order_consumer", "Order.new", 105, "29ms"))
    $scope.consumers.push(new BuildConsumer("TestGroup", "order_test_consumer", "Order.update", 71, "42ms"))


    $scope.brokers = []
    $scope.brokers.push(new BuildBroker("HermesBroker1", "10.3.9.221", 22, "3.5M", 629))
    $scope.brokers.push(new BuildBroker("HermesBroker2", "10.3.9.221", 31, "1.9M", 700))
    $scope.brokers.push(new BuildBroker("HermesBroker3", "10.3.9.221", 29, "2.0M", 531))

})


var BuildTopic = function (name, qps, j, io, latency, etc) {
    var topic = {}
    topic.name = name;
    topic.qps = qps;
    topic.j = j;
    topic.io = io;
    topic.latency = latency;
    topic.etc = etc;
    return topic;
}

var BuildConsumer = function (group, name, topic, qps, latency) {
    var consumer = {}
    consumer.group = group;
    consumer.name = name;
    consumer.topic = topic;
    consumer.qps = qps;
    consumer.latency = latency;
    return consumer;
}

var BuildBroker = function (name, ip, load, io, qps) {
    var broker = {}
    broker.name = name;
    broker.ip = ip;
    broker.load = load;
    broker.io = io;
    broker.qps = qps;
    return broker;
}
