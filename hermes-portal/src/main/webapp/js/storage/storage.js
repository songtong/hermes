var hermes_storage = angular.module('hermes-storage', [ 'ngResource', 'xeditable', 'mgcrea.ngStrap','Storage', 'smart-table', 'components', 'utils', 'bootstrap-tagsinput']);
hermes_storage.run(function(editableOptions) {
	editableOptions.theme = 'bs3';
}).controller('storage-controller', [ '$scope', '$resource', 'StorageService', 'promiseChain', function(scope, resource, StorageService, promiseChain) {
	// Define resource.
	var meta_resource = resource('/api/storages', {}, {
		'get_storages' : {
			method : 'GET',
			isArray : true
		}
	});
	
	var tagResource = resource('/api/tags', {}, {
		'getTags': {
			url: '/api/tags',
			method: 'GET'
		},
		'getDatasourceTags': {
			url: '/datasources/:id',
			method: 'GET'
		},
		'addDatasourceTags': {
			url: '/datasources/:id',
			method: 'POST'
		}
	}); 
	
	// All datasources.
	scope.__datasources = null;
	// Selected datasources.
	scope.datasources = null;
	
	scope.currentDatasource = null;
	
	scope.tags = [];
	
	//
	scope.storageType = 'kafka';
	
	scope.selectedTags = null;

	// Init.
    (function() {
        meta_resource.get_storages({}, function (data) {
            scope.__datasources = data;
            scope.datasources = data[0];
            scope.selectStorage(scope.storageType);
        });
        
//        tagResource.getTags(function(result){
//        	var tmp = [];
//        	$.each(result.data['0'], function(group, tags) {
//        		tmp = tmp.concat(tags);
//        		scope.tags = tmp;
//        	});
//        });
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
    	$('#datasource-modal').modal();
    };
    
    scope.reset = function($event) {
    	$($event.currentTarget).parents('.modal-footer').siblings('.modal-body').find('form input').val('');
    };
    
    scope.add = function() {
    	scope.currentDatasource = {created: true};
    };
    
    scope.save = function() {
    	if (scope.currentDatasource.created) {
        	StorageService.add_datasource(scope.currentDatasource, scope.storageType, function(){
    			scope.datasources.push(scope.currentDatasource);
    			tagResource.addDatasourceTags({id: scope.currentDatasource.id, tagId: '' }, function(){
    				
    			});
    			
//    			$('<tr>').append($('<td>').text($scope.currentDatasource.id))
//        			.append($('<td>').text($scope.currentDatasource.properties['user']))
//        			.append($('<td>').text($scope.currentDatasource.properties['password']))
//        			.append($('<td>').text($scope.currentDatasource.properties['url']))
//        			.append($('<td>').text($scope.currentDatasource.properties['minimumSize']))
//        			.append($('<td>').text($scope.currentDatasource.properties['maximumSize']))
//        			.append($('<td>').text($scope.currentDatasource.id));
        	});
    	} else {
    		StorageService.update_datasource(scope.storageType, scope.currentDatasource.id, scope.currentDatasource);
    	}

    };
    
    scope.remove = function(context) {
    	StorageService.delete_datasource(scope.datasources[context.index].id, scope.storageType, function(){
        	scope.datasources.splice(context.index, 1);
        	// Angular bug: can not handle collection changes when using ng-repeat in template.
        	$(context.target).parents('tr').remove();
    	});
    };
    
    scope.confirm = function($index, $event) {
    	scope.$broadcast('confirm', 'confirmDialog', {index: $index, target: $event.currentTarget});
    };
    
//    scope.tagClass = function (item) {
//    	var classes = ['label-success', 'label-info', 'label-danger', 'label-warning'];
//    	var $this = $(this);
//    	if ($this.data('item') == undefined) {
//    		$this.data('item', 0);
//    	} else {
//    		$this.data('item', ($this.data('item') + 1) % classes.length);
//    	}
//    	return 'label ' + classes[$this.data('item')];
//    } 
    
    scope.selected = function(data) {
    	scope.selectedTags = data;
    }
    
} ]);



