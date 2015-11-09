var application_module = angular.module('application', [ 'ngResource', 'ngRoute', 'xeditable', 'smart-table' ]);

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
	})
});

application_module.filter('short', function() {
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
});

application_module.service('ApplicationService', [ '$resource', '$q', function($resource, $q) {

	var application_resource = $resource("/api/applications/", {}, {
		create_topic_application : {
			method : 'POST',
			url : '/api/applications/topic/create'
		},
		get_application : {
			method : 'GET',
			url : '/api/applications/review/:id'
		},

		get_applications : {
			method : 'GET',
			url : '/api/applications/:status',
			isArray : true
		},

		get_generated_application : {
			method : 'GET',
			url : '/api/applications/generated/:id'
		}
	});

	return {
		'create_topic_application' : function(content) {
			var delay = $q.defer();
			application_resource.create_topic_application(content, function(result) {
				delay.resolve(result);
			}, function(result) {
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
		'get_applications' : function(app_status) {
			var delay = $q.defer();
			application_resource.get_applications({
				status : app_status
			}, function(result) {
				console.log(result);
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
		}
	}
} ]);
