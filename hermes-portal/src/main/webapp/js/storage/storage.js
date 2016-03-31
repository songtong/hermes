var hermes_storage = angular.module('hermes-storage', [ 'ngResource', 'xeditable', 'mgcrea.ngStrap','Storage', 'smart-table', 'components', 'utils', 'bootstrap-tagsinput']);
hermes_storage.run(function(editableOptions) {
	editableOptions.theme = 'bs3';
}).controller('storage-controller', [ '$scope', '$resource', 'StorageService', 'promiseChain', 'watcher', function(scope, resource, StorageService, promiseChain, watcher) {
	// Define resource.
	var meta_resource = resource('/api/storages', {}, {
		'get_storages' : {
			method : 'GET',
			isArray : true
		}
	});
	
	// Define tag resource.
	var tagResource = resource('/api/tags', {}, {
		'addTag': {
			url: '/api/tags',
			method: 'POST'
		},
		'getTags': {
			url: '/api/tags',
			method: 'GET'
		},
		'getDatasourcesTags': {
			url: '/api/tags/datasources',
			method: 'GET'
		},
		'getDatasourceTags': {
			url: '/api/tags/datasources/:id',
			method: 'GET'
		},
		'addDatasourceTag': {
			url: '/api/tags/datasources/:id',
			method: 'POST'
		},
		'removeDatasourceTag': {
			url: '/api/tags/datasources/:id',
			method: 'DELETE'
		}
	}); 
	
	// All datasources.
	scope.__datasources = null;
	// Selected datasources.
	scope.datasources = null;
	
	scope.currentDatasource = null;
	
	scope.datasourcesTags = {};
	
	scope.groupTags = {'test': [{id:2, name: 'd'}]};
	//
	scope.storageType = 'kafka';
	
	scope.newTag = null;
	
	scope.newTagGroup = null;
	
	scope.addedTags = [];
	scope.removedTags = [];

	// Init.
    (function() {
    	promiseChain.add({
    		func: meta_resource.get_storages,
    		args: {},
    		success: function(data){
              scope.__datasources = data;
              scope.datasources = data[0];
              scope.selectStorage(scope.storageType);
    		}
    	}, true).add({
    		func: tagResource.getDatasourcesTags,
    		args: {},
    		success: function(result) {
    			scope.datasourcesTags = result.data[0];
    		}
    	}, true).add({
    		func: tagResource.getTags,
    		args: {},
    		success: function(result) {
    			scope.groupTags = result.data[0];
    			scope.$emit('initialized');
    		}
    	}, true).finish();
    })();
    
    scope.selectStorage = function(type) {
    	scope.storageType = type;
    	if (type == 'mysql') {
    		scope.datasources = scope.__datasources[1].datasources;
    	} else {
    		scope.datasources = scope.__datasources[0].datasources;
    	}
    }
    
    scope.edit = function(index) {
    	scope.currentDatasource = scope.datasources[index];
    	
    	// Clear tags.
    	scope.$broadcast('select2:clear', 'tags');
    	
    	// Init tags control.
    	if (scope.datasourcesTags[scope.currentDatasource['id']]) {
    		scope.$broadcast('select2:init', 'tags', scope.datasourcesTags[scope.currentDatasource['id']].map(function(elem, index){
    			return elem.id;
    		}));
    	}
    	
    	// Show modal. 
    	$('#datasource-modal').modal();
    };
    
    // Reset all form contents & tags.
    scope.reset = function($event) {
    	// Reset all form controls.
    	$($event.currentTarget).parents('.modal-footer').siblings('.modal-body').find('form input').val('');
    	
    	// Clear Tags.
    	scope.$broadcast('select2:clear', 'tags');
    	scope.$broadcast('select2:clear', 'groups');
    };
    
    scope.add = function() {
    	scope.currentDatasource = {onCreate: true};

    	// Clear Tags.
    	scope.$broadcast('select2:clear', 'tags');
    };
    
    scope.save = function() {
    	var tagOps = [];
    	
    	for (var index in scope.addedTags) {
    		tagOps.push({
    			func: tagResource.addDatasourceTag,
    			args: [{id: scope.currentDatasource.id, tagId: scope.addedTags[index]}, null]
    		});
    	}
    	
    	for (var index in scope.removedTags) {
    		tagOps.push({
    			func: tagResource.removeDatasourceTag,
    			args: [{id: scope.currentDatasource.id, tagId: scope.removedTags[index]}, null]
    		});
		}
    	
    	if (scope.currentDatasource.onCreate) {
        	StorageService.add_datasource(scope.currentDatasource, scope.storageType, function(){
        		// Remove flag for on creating datasource.
        		delete scope.currentDatasource.onCreate;
        		
    			scope.datasources.push(scope.currentDatasource);
    			
    			// Leverage promise chain to finish adding tags.
    			promiseChain.newBorn()
    				.add(tagOps, true)
    				.add({
    		    		func: tagResource.getDatasourcesTags,
    		    		args: [],
    		    		success: function(result) {
    		    			scope.datasourcesTags = result.data[0];
    		    		}
    		    	}, true).finish();
    			
//    			$('<tr>').append($('<td>').text($scope.currentDatasource.id))
//        			.append($('<td>').text($scope.currentDatasource.properties['user']))
//        			.append($('<td>').text($scope.currentDatasource.properties['password']))
//        			.append($('<td>').text($scope.currentDatasource.properties['url']))
//        			.append($('<td>').text($scope.currentDatasource.properties['minimumSize']))
//        			.append($('<td>').text($scope.currentDatasource.properties['maximumSize']))
//        			.append($('<td>').text($scope.currentDatasource.id));
        	});
    	} else {
    		StorageService.update_datasource(scope.storageType, scope.currentDatasource.id, scope.currentDatasource, function() {
    			promiseChain.newBorn()
				.add(tagOps, true)
				.add({
		    		func: tagResource.getDatasourcesTags,
		    		args: [],
		    		success: function(result) {
		    			scope.datasourcesTags = result.data[0];
		    		}
		    	}, true).finish();
    		});
    	}

    };
    
    // Datasource removal.
    scope.remove = function(context) {
    	scope.currentDatasource = scope.datasources[context.index];
    	var tags = scope.datasourcesTags[scope.currentDatasource.id];

    	// If having tags attached to this ds, delete first.
    	if (tags) {
    		var tagRemovals = [];
    		var w = watcher.register({
    			context: {count: 0},
    			step: function() {
    				if (++this.count == tags.length) {
    					return true;
    				}
    				return false;
    			},
    			handlers: [function() {
    				doRemove();
    			}]
    		});
    		for (var index in tags) {
        		tagRemovals.push({
        			func: tagResource.removeDatasourceTag,
        			args: [{id: scope.currentDatasource.id, tagId: tags[index].id}, null],
        			success: function() {
        				w.step();
        			}
        		});
    		}
    		promiseChain.newBorn().add(tagRemovals, true).finish();
    	} else {
    		doRemove();
    	}
    	
    	function doRemove() {
    		StorageService.delete_datasource(scope.currentDatasource.id, scope.storageType, function(){
	        	scope.datasources.splice(context.index, 1);
	        	// Angular bug: can not handle collection changes when using ng-repeat in template.
	        	//$(context.target).parents('tr').remove();
	    		promiseChain.newBorn()
	    			.add({
			    		func: tagResource.getDatasourcesTags,
			    		args: [],
			    		success: function(result) {
			    			scope.datasourcesTags = result.data[0];
			    		}
			    	}, true).finish();

	    	});
    	}
    };
    
    // Raise confirm dialog.
    scope.confirm = function($index, $event) {
    	scope.$broadcast('confirm', 'confirmDialog', {index: $index, target: $event.currentTarget});
    };
    
    // Tags change event handler.
    scope.onChange = function(data) {
    	scope.addedTags = [];
		scope.removedTags = [];
		if (!data) {
			data = [];
		}
		
		var tags = scope.datasourcesTags[scope.currentDatasource.id];
		if (tags) {
			$.each(tags, function(index, tag) {
				var filtered = data.filter(function(t) {
					return t == String(tag.id);
				});
				if (filtered.length == 0) {
					scope.removedTags.push(tag.id);
				}
			});
			
			$.each(data, function(index, t) {
				var filtered = tags.filter(function(tag) {
					return t == String(tag.id);
				});
				
				if (filtered.length == 0) {
					scope.addedTags.push(t);
				}
			})
			
		} else {
			scope.addedTags = data;
		}
    };
    
    scope.dataHandler = function(result) {
    	var data = [];
		$.each(result.data[0], function(group, tags){
			$.each(tags, function(index, tag){
				tag.text = tag.name;
				data.push(tag);
			});
		});
		return data;
    };
    
    scope.groupHandler = function(result) {
		return Object.keys(result.data[0]);
    };
    
    scope.selectGroup = function(data) {
    	if (data) {
    		scope.newTagGroup = data;
    	}
    };
    
    scope.onValidate = function(newSelected, selected) {
    	
    };
    
    scope.addTag = function() {
    	var chain = promiseChain.newBorn().add({
    		func: tagResource.addTag,
    		args: [{
    			name: scope.newTag,
    			group: scope.newTagGroup
    		}],
    		success: function() {
    			scope.newTag = null;
    			scope.newTagGroup = null;
    		}
    	}, true).add({
    		func: tagResource.getTags,
    		success: function(result) {
    			scope.groupTags = result.data[0];
    			scope.$broadcast('select2:data', 'tags');
    			scope.$broadcast('select2:clear', 'groups');
    		}
    	}, true).finish();
    }
    
//    scope.onSelect = function(tag) {
//    	var tags = scope.datasourcesTags[scope.currentDatasource.id];
//    	if (tags) {
//    		var filtered = tags.filter(function(t, i){
//    			return t.id == tag.id;
//    		});
//    		if (filtered.length == 0) {
//    			scope.tagsAdded.push(tag);
//    		}
//    	} else {
//    		scope.tagsAdded.push(tag);
//    	}
//    };
//    
//    scope.onUnselect = function(tag) {
//    	var tags = scope.datasourcesTags[scope.currentDatasource.id];
//    	if (tags) {
//    		var filtered = tags.filter(function(t, i){
//    			return t.id == tag.id;
//    		});
//    		if (filtered.length == 0) {
//    			scope.tagsRemoved.push(tag);
//    		}
//    	}
//    };
} ]);



