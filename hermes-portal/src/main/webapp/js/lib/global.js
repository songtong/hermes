/*
 * Global module service.
 * dependency: []
 */
angular.module('global', []).provider('_config', [function(){
	var __config = null;
	
	this.setConfig = function(config) {
		if (!__config) {
			__config = config;
			return;
		}
		throw new Error('Global config already initialized!');
	};
	
	this.$get = [function() {
		return new function() {
			this.getConfig = function(key) {
				if (key) {
					return __config[key];
				}
				return __config;
			};
		};
	}];
}]).config(['_configProvider', function(_configProvider){
	$.ajax({
		method: 'GET',
		url: '/js/lib/global.json',
		dataType: 'json',
		success: function(data) {
			_configProvider.setConfig(data);
		},
		error: function(){
			throw new Error('Load global config file failed!');
		}
	});
}]).service('config', ['_config', function(_config){
	return _config;
}]);