/*
 * Module utils
 * @author: uknow.
 */
angular.module('utils', []).service('watcher', ['$http', function($http) {
	var registry = {};
	
	// Register watcher.
	this.register = function() {
		if (arguments.length < 2) {
			throw new Error('Parameters [id, def] are required!');
		} 
		
		var id = arguments[0];
		var def = arguments[1];
		
		if (registry[id]) {
			throw new Error('Watcher with id [' + id + '] is already existed!');
		} 
		
		// Register new watcher into registry.
		registry[id] = {
			context: def['context']? def['context']: {},
			step: def['step']? function() {
				console.log(def['step'].toString());
				if (def['step'].apply(this['context'])) {
					for (var index in this['handlers']) {
						this['handlers'][index].apply(this['context']);
					}
				}
			}: function() {
				return true;
			},
			handlers: def['handlers'],
			registerHandler: function() {
				this.registerHandler.call(this, id, arguments[0])
			}.bind(this)
		};
		
		return $(registry[id]).on('step', function(e) {
			e.stopPropagation();
			e.preventDefault();
			console.log(e.target);
			e.target.step.apply(e.target);
		}).get(0);
	}.bind(this);
	
	// Step change.
	this.step = function() {
		if (arguments.length < 1) {
			throw new Error('Parameter [id] is required!');
		} 
		registry[arguments[0]].step.apply(registry[arguments[0]]);
	}
	
	// Event step change.
	$(this).on('step', function() {
		if (arguments.length < 2) {
			throw new Error('Parameters [event, id] are required!');
		}
		this.step(arguments[1]);
	}.bind(this));
	
	// Unregister watcher.
	this.unregister = function() {
		if (arguments.length < 1) {
			throw new Error('Parameter [id] is required!');
		} 
		delete registry[arguments[0]];
	};
	
	// Register new handler to the existed watcher.
	this.registerHandler = function() {
		if (arguments.length < 2) {
			throw new Error('Parameters [id, handler] are required!');
		} 
		
		var id = arguments[0];
		var handler = arguments[1];
		
		if (typeof(handler) != 'function') {
			throw new Error('Parameter handler MUST be an function!');
		}
		
		if (!registry[id]) {
			throw new Error('Watcher with id [' + id + '] is NOT existed!');
		} 
		registry[id]['handlers'].push(handler);
	};
}]);