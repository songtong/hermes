angular.module('hermes-resender', [ 'ngResource' ]).controller('resender-controller', [ '$scope', '$resource', function(scope, resource) {
	var topic_resource = resource('/api/topics/:name', {}, {
		get_topic_names : {
			method : 'GET',
			isArray : true,
			url : '/api/meta/topics/names'
		},
		send_message : {
			method : 'POST',
			url : '/api/topics/:name/send'
		}
	});

	topic_resource.get_topic_names({}, function(data) {
		$('#input_topic').typeahead({
			name : 'topics',
			source : substringMatcher(data)
		});
	});

	scope.send_message = function send_message() {
		console.log(scope.topic);
		console.log(scope.message_content);
		topic_resource.send_message({
			name : scope.topic
		}, scope.message_content, function() {
			show_op_info.show("发送消息成功！");
			scope.topic = "";
			scope.message_content = "";
		});
	}
} ]);