var application_module = angular.module('application', [ 'ngResource', 'ngRoute', 'xeditable', 'smart-table', 'ui.bootstrap', 'components', 'utils', 'TopicSync', 'user']);

application_module.run(function(editableOptions) {
	editableOptions.theme = 'bs3';
});

application_module.config(function($routeProvider) {
	$routeProvider.when('/topic', {
		templateUrl : '/jsp/console/application/application-topic.html',
		controller : 'app-topic-controller'
	}).when('/consumer', {
		templateUrl : '/jsp/console/application/application-consumer.html',
		controller : 'app-consumer-controller'
	}).when('/review/:id', {
		templateUrl : '/jsp/console/application/application-review.html',
		controller : 'app-review-controller'
	}).when('/approval/list', {
		templateUrl : '/jsp/console/application/application-approval-list.html',
		controller : 'app-approval-list-controller'
	}).when('/approval/:id', {
		templateUrl : '/jsp/console/application/application-approval-detail.html',
		controller : 'app-approval-detail-controller'
	});
}).filter('short', function() { // define global short filter.
	return function(input, length) {
		input = input || '';
		length = length || 30;
		input = input.replace(/\\"/g, '"');
		if (input.length <= length) {
			return input;
		}
		out = input.substring(0, length / 2) + " ... " + input.substring(input.length - length / 2);
		return out;
	}
}).service('ApplicationService', [ '$resource', '$q', function($resource, $q) { // define application service.
	
	var application_resource = $resource("/api/applications/", {}, {
		create_topic_application : {
			method : 'POST',
			url : '/api/applications/topic/create'
		},
		create_consumer_application : {
			method : 'POST',
			url : '/api/applications/consumer/create'
		},
		get_application : {
			method : 'GET',
			url : '/api/applications/review/:id'
		},

		get_applications : {
			method : 'GET',
			url : '/api/applications/:status'
		},

		get_generated_application : {
			method : 'GET',
			url : '/api/applications/generated/:id'
		},
		
		get_generated_application_by_type : {
			method : 'POST',
			url : '/api/applications/generatedByType/:type',
			params : {
				type : '@type'
			},
			transformRequest: function(data, headers) {
				return JSON.stringify(data);
			}
		},
		
		update_application : {
			method : 'PUT',
			url : '/api/applications/update/:type',
			params : {
				type : '@type'
			},
			transformRequest: function(data, headers) {
				return JSON.stringify(data);
			}
		},
		reject_application : {
			method : 'PUT',
			url : '/api/applications/reject/:id',
			params : {
				id : '@id',
				comment : '@comment',
				approver : '@approver'
			}
		},
		pass_application : {
			method : 'PUT',
			url : '/api/applications/pass/:id',
			params : {
				id : '@id',
				comment : '@comment',
				approver : '@approver'
			},
			transformRequest: function(data, headers) {
				return JSON.stringify(data.polished);
			}
		},
		update_application_status : {
			method : 'PUT',
			url : '/api/applications/status/:id',
			params : {
				id : '@id',
				status: '@status',
				comment : '@comment',
				approver : '@approver'
			},
			transformRequest: function(data, headers) {
				return JSON.stringify(data.polished);
			}
		}
	});

	var topic_resource = $resource('/api/topics/:name', {}, {
		get_topic_names : {
			method : 'GET',
			isArray : true,
			url : '/api/topics/names'
		},
		deploy_topic : {
			method : 'POST',
			isArray : false,
			url : '/api/topics/:name/deploy',
			params : {
				name : '@name'
			}
		},
		sync_topic : {
			method : 'POST',
			isArray : false,
			url : '/api/topics/sync',
			params : {
				environment : '@environment',
				force_schema : '@forceSchema'
			}
		}
	});
	
	consumer_resource = $resource('/api/consumers/:topic/:consumer', {}, {
		add_consumer : {
			method : 'POST',
			url : '/api/consumers/'
		}
	});

	return {
		'add_consumer' : function(consumer) {
			var delay = $q.defer();
			consumer_resource.add_consumer(consumer, function(result) {
				delay.resolve(result);
			}, function(result) {
				result.consumer = consumer;
				delay.reject(result);
			});
			return delay.promise;
		},
		'add_consumers' : function(consumers) {
			var delay = $q.defer();
			var success_topics = [];
			var failed_topics = [];
			var count = 0;
			for (var i = 0; i < consumers.length; i++) {
				consumer_resource.add_consumer(consumers[i], function(result) {
					success_topics.push(result.topicName);
					count = count + 1;
					console.log("count = " + count + " topicName = " + result.topicName);
					if (count == consumers.length) {
						var result = {
							'success_topics' : success_topics,
						};
						delay.resolve(result);
					}
				}, function(result) {
					count = count + 1;
					console.log("count = " + count);
					if (count == consumers.length) {
						var result = {
							'success_topics' : success_topics,
						};
						delay.resolve(result);

					}
				});
			}
			return delay.promise;

		},
		'create_topic_application' : function(content) {
			var delay = $q.defer();
			application_resource.create_topic_application(content, function(result) {
				delay.resolve(result);
			}, function(result) {
				delay.reject(result);
			});
			return delay.promise;
		},
		'create_consumer_application' : function(content) {
			console.log("application_servie.create_consumer_application");
			var delay = $q.defer();
			application_resource.create_consumer_application(content, function(result) {
				delay.resolve(result);
			}, function(result) {
				console.log(delay);
				delay.reject(result);
			});
			return delay.promise;
		},
		'get_application' : function(app_id) {
			var delay = $q.defer();
			application_resource.get_application({
				id : app_id
			}, function(result) {
				delay.resolve(result);
			}, function(result) {
				delay.reject(result);
			});
			return delay.promise;
		},
		'get_applications' : function(status, owner, offset, size) {
			var delay = $q.defer();
			application_resource.get_applications({
				status : status,
				owner: owner,
				offset: offset,
				size: size
			}, function(result) {
				delay.resolve(result);
			}, function(result) {
				delay.reject(result);
			});
			return delay.promise;
		},
		'get_generated_application' : function(app_id) {
			var delay = $q.defer();
			application_resource.get_generated_application({
				id : app_id
			}, function(result) {
				delay.resolve(result);
			}, function(result) {
				delay.reject(result);
			})
			return delay.promise;
		},
		'update_application' : function(app) {
			var delay = $q.defer();
			application_resource.update_application( app, function(result) {
				console.log("application_resource.update_application success");
				delay.resolve(result);
			}, function(result) {
				console.log("application_resource.update_application failed");
				delay.reject(result);
			});
			return delay.promise;
		},
		'reject_application' : function(app_id, app_comment, app_approver) {
			console.log("ApplicationService.reject_application");
			var delay = $q.defer();
			application_resource.reject_application({
				id : app_id,
				comment : app_comment,
				approver : app_approver
			}, function(result) {
				console.log("ApplicationService.reject_application.success");
				delay.resolve(result);
			}, function(result) {
				console.log(result);
				console.log("ApplicationService.reject_application.failed");
				delay.reject(result);
			});
			return delay.promise;
		},
		'pass_application' : function(app_id, app_comment, app_approver, polished_content) {
			var delay = $q.defer();
			application_resource.pass_application({
				id : app_id,
				comment : app_comment,
				approver : app_approver,
				polished: polished_content
			}, function(result) {
				delay.resolve(result);
			}, function(result) {
				delay.reject(result);
			});
			return delay.promise;
		},
		'update_application_status': function(app_id, app_status, app_comment, app_approver, polished_content) {
			var delay = $q.defer();
			application_resource.update_application_status({
				id : app_id,
				status: app_status,
				comment : app_comment,
				approver : app_approver,
				polished: polished_content
			}, function(result) {
				delay.resolve(result);
			}, function(result) {
				delay.reject(result);
			});
			return delay.promise;
		},
		'get_productLines' : function() {
			return [ 'fx', 'hotel', 'flight' ];
		},
		'get_topic_names' : function(id) {
			var delay = $q.defer();
			topic_resource.get_topic_names({}, function(result) {
				delay.resolve(result);
			}, function(result) {
				delay.reject(result);
			});
			return delay.promise;
		},
		'deploy_topic' : function(name) {
			var delay = $q.defer();
			topic_resource.deploy_topic({
				name : name
			}, function(result) {
				delay.resolve(result);
			}, function(result) {
				delay.reject(result);
			});
			return delay.promise;
		},
		'sync_topic' : function(topic, environment, forceSchema) {
			var delay = $q.defer();
			console.log(topic);
			topic_resource.sync_topic({
				environment : environment,
				forceSchema : forceSchema
			}, topic, function(result) {
				delay.resolve(result);
			}, function(result) {
				delay.reject(result);
			});
			return delay.promise;
		},
		'remove_topic' : function(topicName) {
			var delay = $q.defer();
			topic_resource.remove({
				name : topicName
			}, function(result) {
				delay.resolve(result);
			}, function(result) {
				delay.reject(result);
			});
			return delay.promise;
		},
		'create_topic' : function(topic) {
			var delay = $q.defer();
			topic_resource.save(topic, function(result) {
				delay.resolve(result);
			}, function(result) {
				delay.reject(result);
			});
			return delay.promise;
		},
		'get_generated_application_by_type': function(application) {
			var delay = $q.defer();
			application_resource.get_generated_application_by_type(application, function(result) {
				delay.resolve(result);
			}, function(result) {
				delay.reject(result);
			});
			return delay.promise;
		}
	}
} ]);
