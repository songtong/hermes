angular.module('log', ['global']).service('Log',['Config', function(Config){
	var debug = Config.getConfig('debug');
	return {
		log: function() {
			if (debug) {
				console.log.apply(console, arguments);
			}
		},
		info: function() {
			if (debug) {
				console.info.apply(console, arguments);
			}	
		},
		warn: function() {
			if (debug) {
				console.warn.apply(console, arguments);
			}	
		},
		error: function() {
			if (debug) {
				console.error.apply(console, arguments);
			}	
		}
	};
});