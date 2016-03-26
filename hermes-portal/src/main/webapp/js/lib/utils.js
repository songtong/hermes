/*
 * Module utils
 * @author: uknow.
 */
var module = angular.module('utils', ['global']);
/*
 * Page context for self codes debug.
 */
module.provider('_context', [function() {
	var __context = {
		debug: false
	};

	__context['debug'] = $(document.body).attr("debug")
		.trim().toLowerCase() == 'true';

	this.isDebug = function() {
		return __context['debug'];
	};
	
	this.$get = function() {
		return __context;
	};
}]).service('context', ['_context', function(_context){
	return _context;
}]);

/*
 * Clone service
 * Supporting deep clone.
 * dependency: []
 */
module.service('clone', function() {
	this.copy = function(obj, deep) {
		if (typeof(obj) == 'obj') {
			if (obj instanceof Array) {
				var tmp = [];
				for (var i in obj) {
					tmp.push(deep? this.copy(obj[i], deep): obj[i]);
				}
				return tmp;
			} else {
				var tmp = {};
				Object.keys(obj).forEach(function(key, index){
					tmp[key] = deep? this.copy(obj[key], deep): obj[key];
				}.bind(this));
				return tmp;
			}
		}
		return obj;
	}.bind(this);
 });

/*
 * Cache service.
 * dependency: [logger]
 */
module.service('cache', ['logger', function(logger) {
	var cache = {
		default: {}
	};
	
	return new function() {
		this.__validate = function(key) {
			if (typeof(key) == 'object') {
				throw new Error('Cache key can NOT be object!');
			}
			return true;
		};
		
		this.set = function(key, value, group) {
			if (this.__validate(key)) {
				if (!group) {
					group = 'default';
				}
				var existed = cache[group][key];
				cache[group][key] = value;
				logger.log('cache', 'add kv pair [' + key + ',' + JSON.stringify(value)+ '] to cache group [' + group + ']');
				return existed;
			}
		};
		
		this.unset = function(key, group) {
			if (this.__validate(key)) {
				if (!group) {
					group = 'default';
				}
				delete cache[group][key];
				logger.log('cache', 'delete kv pair [' + key + ',' + JSON.stringify(value) + '] from cache group [' + group + ']');
			}
		};
	};
}]);
 
/*
 * Watcher service.
 * dependency: []
 */
 module.service('watcher', ['logger', function(logger) {
	var registry = {};
	var logger = logger('watcher');
	var sequence = 0;
	
	// Register watcher.
	this.register = function() {
		if (arguments.length == 0) {
			throw new Error('Parameter [def] is required!');
		} 
		
		var id, def;
		if (arguments.length == 1) {
			id = ++sequence;
			def = arguments[0];
		} else {
			id = arguments[0];
			def = arguments[1];
		}
		
		if (registry[id]) {
			throw new Error('Watcher with id [' + id + '] is already existed!');
		} 
		
		// Register new watcher into registry.
		registry[id] = {
			context: def['context']? def['context']: {},
			step: def['step']? function() {
				logger.log('invoke step method for timing check: ' + def['step']);
				if (def['step'].apply(this['context'])) {
					logger.log('current context after step invoke: ' + JSON.stringify(this['context']));
					logger.log('handlers invoke kickoff: ' + this['handlers'].length);
					for (var index in this['handlers']) {
						this['handlers'][index].apply(this['context']);
					}
					logger.log('handlers invoke done: ' + this['handlers'].length);
				} else {
					logger.log('current context after step invoke: ' + JSON.stringify(this['context']));
				}
			}: function() {
				return true;
			},
			handlers: def['handlers'],
			registerHandler: function() {
				this.registerHandler.call(this, id, arguments[0])
			}.bind(this)
		};
		logger.log('register new watcher: ' + JSON.stringify(registry[id]));
		
		return $(registry[id]).on('step', function(e) {
			e.preventDefault();
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

 /*
  * Logger provider.
  * dependency: [config]
  */
module.provider('logger', ['_contextProvider', function(_contextProvider){
	var debug = _contextProvider.isDebug();

	function withTemplate(func) {
		return function() {
			var args = Array.prototype.slice.apply(arguments);
			args.unshift(this.__name);
			args.unshift('[%s] %s');
			if (debug) {
				func.apply(console, args);
			}
		};
	}
	this.$get = function(){
		return function(name) {
			return new function() {
				this.__name = name;
				this.log = withTemplate(console.log);
				this.info = withTemplate(console.info);
				this.warn = withTemplate(console.warn);
				this.error = withTemplate(console.error);
			};
		};
	};
}]);

module.service('promiseChain', ['logger', function(logger) {
	var logger = logger('promiseChain');
	
	function promise(id, func, args, success, error) {
		this.__context = {};
		this.__id = id;
		this.__next = null;
		this.__abort = false;
		this.__callback = null;
		
		this.call = function(context) {
			if (!context) {
				context = {};
			}

			var withContext = function() {
				var args = Array.prototype.slice.apply(arguments);
				args.push(this.__context);
				return args;
			}
			var withArgs = function() {
				var args = [];
				for (var index = 0; index < arguments.length; index++) {
					if (typeof(arguments[index]) == 'function') {
						args.push(arguments[index].apply(context));
					} else if (typeof(arguments[index]) == 'string' && arguments[index].trim().indexOf('@') == 0) {
						args.push(context[arguments[index].trim().substr(1)]);
					} else {
						args.push(arguments[index]);
					}
				}
				return args;
			}
			
			this.__context = { 
				abort: function() {
					this.__abort = true;
				}.bind(this),
			
				resume: function() {
					this.__abort = false;
				}.bind(this)
			};

			func.apply(this, withArgs.apply(context, args)).then(function() {
				this.__context.resume();
				if (success) {
					logger.log('promise invoke success method: ' + success);
					success.apply(this.__context, withContext.apply(this, arguments));
				}
				this.next();
			}.bind(this), function(){
				this.__context.abort();
				if (error) {
					logger.log('promise invoke error method: ' + success);
					error.apply(this.__context, arguments);
				}
				this.next();
			}.bind(this));
		}.bind(this);
		
		this.setCallback = function(callback) {
			this.__callback = callback;
		}
		
		this.next = function() {
			$(this).trigger('step');
			if (this.__next && !this.__abort) {
				this.__next.call.call(this.__next, this.__context);
			} else {
				this.__callback.apply(this);
			}
		};
		
		this.setNext = function(next) {
			this.__next = next;
		}.bind(this);
		
		this.getNext = function() {
			return this.__next;
		}.bind(this);
		
		this.setContext = function(key, value) {
			this.__context[key] = value;
		};
	}
	
	function promises() {
		this.__proto__ = new promise();
		
		this.pList = [];
		
		this.add = function(p) {
			this.pList.push(p);
			logger.log('add promises into chain: ' + JSON.stringify(p));
		}.bind(this);

		this.call = function(context) {
			logger.log('promises call kickoff: ' + this.pList.length);
			var counter = 0;
			for (var index = 0; index < this.pList.length; index++) {
				$(this.pList[index]).bind('step', function(){
					if (++counter == this.pList.length && this.__next) {
						this.__next.call.call(this.__next, this.__context);
					}
				}.bind(this));
				logger.log('promise call kickoff: ' + JSON.stringify(this.pList[index]));
				this.pList[index].call.call(this.pList[index], context);
			}
		}.bind(this);
	}

	return new function() {
		this.sequence = 0;
		this.head = null;
		this.tail = null;
		this.__validate = function(def) {
			if (!def) {
				throw new Error('Definition to one chain MUST be offered!');
			}
			if (typeof(def['func']) != 'function') {
				throw new Error('Parameter [func] MUST be a function!');
			}
			if (!def['id']) {
				def['id'] = ++this.sequence;
			}
			return true;
		};
		
		var callback = function() {
			logger.log('=> finish chain calling: ' + JSON.stringify(this));
		}.bind(this);
		
		this.add = function(def) {
			var node = null;
			if (def instanceof Array) {
				node = new promises();
				for (var index in def) {
					if (this.__validate(def[index])) {
						node.add(
							new promise(def[index]['id'], def[index]['func'], def[index]['args'], def[index]['success'], def[index]['error'])
						);
					}
				}
			} else {
				if (this.__validate(def)) {
					node = new promise(def['id'], def['func'], def['args'], def['success'], def['error']);
				}
			}
			
			if (node) {
				node.setCallback(callback);
				if (this.head) {
					this.tail.setNext(node);
				} else {
					this.head = node;
				}	
				this.tail = node;
				logger.log('add promise/promises to chain: ' + JSON.stringify(this.tail));
			}
			
			return this;
		};
		
		this.finish = function() {
			if (!this.head) {
				throw new Error('Can NOT finish calling a chain without promise!');
			}
			logger.log('=> start chain calling: ' + JSON.stringify(this));
			this.head.call.apply(this);
		};
		
		this.remove = function(id) {
			var current = this.head;
			var prev = null;
			while (current) {
				if (current.id == id) {
					if (prev) {
						prev.setNext(current.getNext());
					} else {
						this.head = this.head.getNext();
					}
					logger.log('remove chain promise node: ' + JSON.stringify(current));
					return current;
				}
				prev = current;
				current = current.getNext();
			}
			logger.log('no promise node found with id: ' + id);
		};
		
		this.removeHead = function() {
			if (!this.head) {
				throw new Error('Can NOT remove head of empty chain!');
			}
			this.head = this.head.getNext();
		};
		
		this.removeTail = function() {
			if (!this.tail) {
				throw new Error('Can NOT remove head of empty chain!');
			}
			
			var current = this.head;
			var prev = null;
			while (current) {
				if (current == this.tail) {
					if (current == this.head) {
						this.head = this.tail = null;
					} else {
						prev.setNext(current.getNext());
					}
					return current;
				}
				prev = current;
				current = current.getNext();
			}
		};
		
		this.clear = function() {
			this.head = this.tail = null;
		};
		
		this.newBorn = function() {
			return new this.constructor;
		}.bind(this);
	};
}]);


