var dashtopic_detail = angular.module("dash-topic-detail", [ 'ngResource', 'ui.bootstrap', 'smart-table' ]);

dashtopic_detail.service("DashtopicDetailService", [ '$filter', '$resource', function() {
	var topic_delays = [];
	var topic_detail_resource = $resource("/api/monitor/", {}, {
		get_topic_delay : {
			method : "GET",
			isArray : true,
			url : "/api/monitor/detail/topics/:topic/delay"
		}
	});
	return {
		get_topic_delays : function() {
			return topic_delays;
		},
		update_topic_delays : function(topic) {
			topic_detail_resource.get_topic_delay({
				'topic' : topic
			}, function(data) {
				console.log(data);
				topic_delays = data;
			});
		}
	};
} ]).controller("dash-topic-detail-controller", [ function($scope, $filter, $resource, DashtopicDetailService) {
	$scope.topic_delays = [];
	DashtopicDetailService.update_topic_delays();
	console.log(cur_topic);
} ]);