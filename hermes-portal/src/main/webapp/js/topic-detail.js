function find_datasource_names(data, type) {
	for (var i = 0; i < data.length; i++) {
		if (data[i].type == type) {
			return collect_schemas(data[i].datasources, 'id', false);
		}
	}
}

function find_endpoint_names(data, type) {
	names = [];
	for (var i = 0; i < data.length; i++) {
		if (data[i].type == type) {
			names.push(data[i].id);
		}
	}
	return names;
}

angular.module('hermes-topic-detail', [ 'ngResource', 'xeditable' ]).run(function(editableOptions) {
	editableOptions.theme = 'bs3';
}).controller('topic-detail-controller', [ '$scope', '$resource', function(scope, resource) {
	var topic_resource = resource('/api/topics/:name', {}, {
		get_topic : {
			method : 'GET',
			isArray : false
		}
	});
	var meta_resource = resource('/api/meta/', {}, {
		'get_codecs' : {
			method : 'GET',
			isArray : true,
			url : '/api/meta/codecs'
		},
		'get_storages' : {
			method : 'GET',
			isArray : true,
			url : '/api/meta/storages'
		},
		'get_endpoints' : {
			method : 'GET',
			isArray : true,
			url : '/api/meta/endpoints'
		}
	});

	scope.codec_types = meta_resource.get_codecs({}, function(result) {
		scope.codec_types = collect_schemas(result, 'type', true);
	});

	scope.storage_types = meta_resource.get_storages({}, function(result) {
		scope.src_storages = result;
		scope.storage_types = collect_schemas(result, 'type', true);
	});

	meta_resource.get_endpoints({}, function(result) {
		scope.src_endpoints = result;
		scope.endpoint_types = collect_schemas(result, 'type', true);
	});

	scope.topic = topic_resource.get_topic({
		name : topic_name
	}, function(query_result) {
		scope.topic = query_result;
		scope.topic.createTime = new Date(scope.topic.createTime).toLocaleString();
		scope.topic.lastModifiedTime = scope.topic.lastModifiedTime ? new Date(scope.topic.lastModifiedTime).toLocaleString() : 'Not Set';
		scope.topic.status = scope.topic.status ? scope.topic.status : 'Not Set';
		scope.datasource_names = find_datasource_names(scope.src_storages, scope.topic.storageType);
		scope.endpoint_names = find_endpoint_names(scope.src_endpoints, scope.topic.endpointType);
	});

} ]);