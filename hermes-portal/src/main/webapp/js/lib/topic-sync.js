var module = angular.module('TopicSync', ['ngResource', 'utils']);
module.service('TopicSync', ['$resource', '$q', 'config', function($resource, $q, config) {
	var syncResource = $resource('', {}, {
		getGeneratedApplicationByType: {
			method: 'POST',
			url: 'http://:domain/api/applications/generatedByType/:type',
			params: {
				type: '@data.type',
				domain: '@domain'
			},
			transformRequest: function(data, headers) {
				return JSON.stringify(data.data);
			}
		},
		
		deployKafkaTopic: {
			method: 'POST',
			url: 'http://:domain/api/topics/:name/deploy',
			params: {
				name: '@data.name',
				domain: '@domain'
			}
		},
		
		getTopic:  {
			method: 'GET',
			url: 'http://:domain/api/topics/:name',
			params: {
				name: '@name',
				domain: '@domain'
			}
		},
		
		createTopic: {
			method: 'POST',
			url: 'http://:domain/api/topics',
			params: {
				domain: '@domain'
			},
			transformRequest: function(data, headers) {
				return JSON.stringify(data.data);
			}
		},
		
		getConsumers:  {
			method: 'GET',
			url: 'http://:domain/api/consumers/:topic',
			params: {
				topic: '@topic',
				domain: '@domain'
			},
			isArray: true
		},
		
		addConsumer: {
			method: 'POST',
			url: 'http://:domain/api/consumers',
			params: {
				domain: '@domain'
			},
			transformRequest: function(data, headers) {
				return JSON.stringify(data.data);
			}
		}
	});
	
	var storageResource = $resource('', {}, {
		'getStorage' : {
			method : 'GET',
			url: 'http://:domain/api/storages',
			isArray : true
		}
	});
	
	var wrapFunc = function(func, isGet) {
		return function() {
			var deferred = $q.defer();
			if (arguments.length < 2) {
				throw new Error('Variables [env, data] are required!');
			}

			var domains = config.getConfig('domains');
			var params = [{
				domain: domains[arguments[0]]
			}];
			if (isGet) {
				$.extend(params[0], arguments[1]);
			} else {
				params[0]['data'] = arguments[1];
			}
		
			params.push(function(result) {
				deferred.resolve(result);
			});
			params.push(function(result) {
				deferred.reject(result);
			});
			
			func.apply(this, params);
			return deferred.promise;
		};
	};
	
	this.getGeneratedApplicationByType = wrapFunc(syncResource.getGeneratedApplicationByType);
	this.deployKafkaTopic = wrapFunc(syncResource.deployKafkaTopic);
	this.getTopic = wrapFunc(syncResource.getTopic, true);
	this.createTopic = wrapFunc(syncResource.createTopic);
	this.getStorage = wrapFunc(storageResource.getStorage, true);
	this.addConsumer = wrapFunc(syncResource.addConsumer);
	this.getConsumers = wrapFunc(syncResource.getConsumers, true);
}]);