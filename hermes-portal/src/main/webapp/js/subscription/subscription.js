angular.module('hermes-subscription', [ 'ngResource', 'ui.bootstrap', 'xeditable' ,'smart-table']).run(function(editableOptions) {
	editableOptions.theme = 'bs3';
}).controller('subscription-controller', [ '$scope', '$filter', '$resource', function(scope, filter, resource) {
	var subscription_resource = resource('/api/subscriptions/', {}, {
		get_topic_names : {
			method : 'GET',
			isArray : true,
			url : '/api/meta/topics/names'
		},
		get_consumer_names : {
			method : 'GET',
			isArray : true,
			url : '/api/consumers/:topic'
		},
		get_subscribers : {
			method : 'GET',
			isArray : true
		},
		delete_subscriber : {
			method : 'DELETE',
			url : '/api/subscriptions/:id'
		},
		start_subscriber : {
			method : 'PUT',
			url : '/api/subscriptions/:id/start',
			params : {
				id : '@id'
			}
		},
		stop_subscriber : {
			method : 'PUT',
			url : '/api/subscriptions/:id/stop',
			params : {
				id : '@id'
			}
		}
	});

	scope.selected = {};
	scope.subscribers = [];
	scope.display_subscribers = [].concat(scope.subscribers);

	scope.topic_names = subscription_resource.get_topic_names({}, function(data) {
		scope.topic_names = data;
	});

	scope.consumer_names = subscription_resource.get_consumer_names({}, function(data) {
		scope.consumer_names = collect_schemas(data, 'groupName', true);
	});

	scope.subscribers = subscription_resource.get_subscribers({}, function(data) {
		scope.subscribers = data;
	});

	scope.add_row = function add_row() {
		scope.inserted = {
			name : undefined,
			topic : undefined,
			group : undefined,
			endpoints : undefined,
			id : 0,
		};
		scope.subscribers.push(scope.inserted);
	};

	scope.add_subscriber = function add_subscriber(subscriber,id) {
		console.log(id);
		subscriber.id=id;
		subscription_resource.save(subscriber, function(data) {
			scope.subscribers = subscription_resource.get_subscribers({}, function(data) {
				scope.subscribers = data;
			});

			show_op_info.show("保存Subscription成功", true);
		}, function(error_result) {
			show_op_info.show("保存失败: " + error_result.data, false);
		});
	};

	scope.del_row = function del_row(subscriber) {
		bootbox.confirm("确认删除Subscription?", function(result) {
			if (result) {
				subscription_resource.delete_subscriber({
					id : subscriber.id
				}, function(data) {
					show_op_info.show("删除成功", true);
				}, function(error_result) {
					show_op_info.show("删除失败: " + error_result.data, false);
				});
				var index = scope.subscribers.indexOf(subscriber);
				if (index > -1) {
					scope.subscribers.splice(index, 1);
				}
			}
		});
	};

	scope.start_subscription = function(sb) {
		console.log(sb);
		subscription_resource.start_subscriber({
			'id' : sb.id
		}, function(data) {
			show_op_info.show("Start subscriber success!", true);
			sb.status = "RUNNING";
		}, function(resp) {
			show_op_info.show("Start subscriber failed! " + resp.data, false);
		});
	};

	scope.stop_subscription = function(sb) {
		console.log(sb);
		subscription_resource.stop_subscriber({
			'id' : sb.id
		}, function(data) {
			show_op_info.show("Stop subscriber success!", true);
			sb.status = "STOPPED";
		}, function(resp) {
			show_op_info.show("Stop subscriber failed! " + resp.data, false);
		});
	};
	
	scope.checkName = function(data,id){
		for(var idx =0; idx < scope.subscribers.length; idx++){
			var subscriber =scope.subscribers[idx]; 
			if(subscriber.id==id)
				continue;
			if(subscriber.name==data)
				return "Name already exists!"
		}
	}
} ]);
