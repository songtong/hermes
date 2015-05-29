angular.module('topic-services', [ 'ngResource' ]).factory('TopicService',
		[ '$q', '$filter', '$timeout', '$resource', get_topics = function($q, $filter, $timeout, $resource) {
			return $resource('/api/topics/:name', {}, {
				query : {
					method : 'GET',
					isArray : true
				}
			});
		} ]);