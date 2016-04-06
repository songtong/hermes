/*
 * UI components.
 * @uknow.
 */
var module = angular.module('components', ['utils']);
module.directive('progressbarX', ['$interval', 'logger', function($interval, logger) {
	var logger = logger('progressbarX');
	function embed($element) {
		$element.find('.progress').remove().appendTo($element).end().end().find('.modal-backdrop').remove();
	}
	return {
		restrict: 'E',
		template: '<div class="modal-backdrop in" style="display: none;" >' 
			+ '<div class="progress" style="width: 80%; margin-left: 10%;">'
			+ '<div class="progress-bar progress-bar-striped active" role="progressbar" aria-valuenow="0" aria-valuemin="0" aria-valuemax="100" style="min-width: 2em; width: 0%; color: black;">'
			+ '<div style="position: absolute; width: 80%;"><span class="percentage" style="position: absolute; left: 40%; width: 10%;"> 100%</span><span class="msg" style="position: absolute;">Processing</span></div>'
			+ '</div>'
			+ '</div>'
			+ '</div>',
		link: function($scope, $element, attrs) {
			var __id = attrs['id'];
			var __embedded = attrs['embed'];
			var $container;
			if (__embedded) {
				embed($element);
				$container = $element.find('.progress').css({width: '100%', marginLeft: 0});
			} else {
				$container = $element.find('.modal-backdrop');
			}
			
			$element = $element.find('.progress');
			if ($container) {
				$container.hide();
			}

			if (!__embedded) {
				// Auto detection for position.
				$(window).on('resize', function(e) {
					var height = $(e.target).height() / 2;
					$element.css('marginTop', height - 20);
				}).trigger('resize');
			}

			
			function updateProgress(progress) {
				var $target = $element.find('.progress-bar');
				if (typeof(progress) == 'number') {
					if (progress < $element.data('progress')) {
						return false;
					}
					
					// Update progress value stored.
					$element.data('progress', progress);
					// Update progress bar on ui.
					$target.css('width', progress + '%').find('.percentage').text(progress + '%');
					logger.log('update progress percentage: ' + $element.data('progress'));
				} else if (typeof(progress) == 'string') {
					$target.find('.msg').text(progress);
					logger.log('update progress text: ' + progress);
				} else if (typeof(progress) == 'object'){
					if (progress['percentage'] != undefined) {
						var result = updateProgress(progress['percentage']);
						// if percentage is valid and msg exists, then update msg text. 
						if (result && progress['msg']) {
							updateProgress(progress['msg']);
						} else {
							return result;
						}
					}
				}
				return true;
			}
			
			// Init progress to 0 by default.
			$element.data('progress', 0);
			
			$scope.$on('progress', function(e, id, progress) {
				if (id && id != __id) {
					return;
				}
				
				if ($container && $container.is(':hidden')) {
					$container.show();
				}
				if (progress) {
					updateProgress(progress);
				} else {
					$scope.$emit('progress-step');
				}
			});
			
			$scope.$on('progress-step', function(e, id, step) {
				if (id && id != __id) {
					return;
				}
				
				if (!step) {
					step = 5;
				}
				if ($container && $container.is(':hidden')) {
					$container.show();
				}

				updateProgress($element.data('progress') + step);
			});
			
			$scope.$on('progress-random', function(e, id, timeout, maximum) {
				if (id && id != __id) {
					return;
				}
				
				if (!timeout) {
					timeout = 3000;
				}
				
				if (!maximum) {
					maximum = 100;
				}
				
				$element.data('progress', 0);
				updateProgress({percentage: 0, msg: 'Processing'});
				logger.log('init the progress of progress bar!');
				
				if ($container && $container.is(':hidden')) {
					$container.show();
				}
				
				var times = 0;
				var step = 100 / Math.ceil(timeout / 500);
				
				var task = $interval(function(){
					// If it's aleady done or exceed the maximum value, cancel the interval.
					if ($element.data('progress') >= maximum) {
						$interval.cancel(task);
						return;
					}
					
					var adjustedStep = Math.floor(Math.random() * ((times + 1) * step - $element.data('progress')));
					var upTo = $element.data('progress') + adjustedStep;
					
					// If the next progress value will exceed the maximum value, cancel the interval as well.
					if (upTo >= maximum) {
						$interval.cancel(task);
						return;
					}
					
					updateProgress(upTo);

					times++;
				}, 500, timeout / 500);
			});
			
			$scope.$on('progress-done', function(e, id, callback) {
				if (id && id != __id) {
					return;
				}
				
				updateProgress({percentage: 100, msg: 'Complete'});
				logger.log('event [progress-done] is handled!');
				var handler = setTimeout(function() {
					if ($container) {
						$container.hide();
					}
					
					clearTimeout(handler);
					if (callback) {
						callback.apply($scope);
					}
				}, 500);
			})
			
			$scope.$on('progress-reset', function() {
				$element.data('progress', 0);
				updateProgress({percentage: 0, msg: 'Processing'});
			});
		}
	};
}]).directive('alertX', [function() {
	function embed($element) {
		$element.find('.alert').remove().appendTo($element).end().end().find('.modal').remove();
	}
	
	return {
		restrict: 'E',
		transclude: true,
		scope: {
			title: '@title',
			content: '@content',
			context: '='
		},
		template: '<div class="modal fade" tabindex="-1" role="dialog">'
				+ '  <div class="modal-dialog">'
				+ '    <div class="modal-content">'
				+ '      <div class="modal-header">'
				+ '        <button type="button" class="close" data-dismiss="modal" aria-label="Close"><span aria-hidden="true">&times;</span></button>'
				+ '        <h4 class="modal-title">{{title}}</h4>'
				+ '      </div>'
				+ '      <div class="modal-body">'
				+ '        <div class="alert alert-danger alert-dismissible fade in" role="alert" style="margin-bottom: 0px; display: block;">'
				+ '            <button type="button" class="close" data-dismiss="alert" aria-label="Close"><span aria-hidden="true">×</span></button>'
				+ '            <p style="text-align: left;"><strong class="alert-title">Alert!</strong> <span class="alert-content"></span></p>'
				+ '            <ng-transclude>'
				+ '            </ng-transclude>'
				+ '        </div>'
				+ '      </div>'
				+ '      <div class="modal-footer">'
				+ '        <button type="button" class="btn btn-default" data-dismiss="modal">Close</button>'
				+ '      </div>'
				+ '    </div>'
				+ '  </div>'
				+ '</div>',
				
		link: function($scope, $element, attrs, ctrl, transclude){
			var __embedded = attrs['embed'];
			var container;
			
			if (__embedded) {
				embed($element);
				$container = $element.find('.alert');
			} else {
				$container = $element.find('.modal');
			}
			
			if ($container) {
				$container.hide();
			}
			
			transclude($scope, function(clone, $scope) {
				if (clone.length > 0) {
					$element.find('ng-transclude').siblings('p').remove().end().empty().append(clone);
				}
			});
			
			function alert(id, isError, content, title) {
				if (id && id !=  attrs['id']) {
					return;
				}
				
				var __embedded = attrs['embed'];
				var container;
				
				if (__embedded) {
					$container = $element.find('.alert');
				} else {
					$container = $element.find('.modal');
				}
				
				$elem = $element.find('.alert');
				
				if ($container.is(':hidden')) {
					if (__embedded) {
						$container.show();
					} else {
						$container.modal('show');
					}
				}
				
				var $tmp = $elem.clone(false);
				$tmp.appendTo($elem.hide().parent());
				if (!isError) {
					$tmp.removeClass('alert-danger').addClass('alert-success');
				}
				
				if (title) {
					$tmp.find('.alert-title').text(title);
				}
				
				if (content) {
					$tmp.find('.alert-content').text(content);
				}
				
				$tmp.show();
				
				if (__embedded) {
					// Delay 3 seconds to close.
					setTimeout(function(){
						$tmp.alert('close');
					}, 3000);
				} else {
					$container.one('hidden.bs.modal', function(){
						$tmp.alert('close');
					});
				}
			}
			
			$scope.$on('alert-error', function(e, id, content, title) {
				alert(id, true, content, title);
			});
			
			$scope.$on('alert-success', function(e, id, content, title) {
				alert(id, false, content, title);
			});
		}
	}
}]).directive('loadingX', function() {
	return {
		restrict: 'E',
		scope: {
			delay: '='
		},
		template: '<div class="modal-backdrop in" style="display: block;" >' 
			+ '<i class="fa fa-spinner fa-pulse" style="color: white; font-size: 60px; margin-left: 48%; "></i>'
			+ '</div>',
		link: function($scope, $element, attrs) {
			$(window).on('resize', function(e) {
				var height = $(e.target).height() / 2;
				$element.find('i').css('marginTop', height - 30);
			}).trigger('resize');

			$scope.$on('initialized', function(e){
				$element.delay($scope.delay? $scope.delay: 250).hide();
			});
		}
	};
}).directive('textX', ['$timeout', function($timeout) {
	return {
		restrict: 'A',
		link: function($scope, $element, attrs) {
			var __handler;
			$timeout(function(){
				var text = $element.text();
				if (text.length > 60) {
					$element.text('').append($('<a>').text(text.substr(0, 60) + '...').on('click', function(e){
						e.preventDefault();
						$element.popover({
							template: '<div class="popover" role="tooltip"><div class="arrow"></div><h3 class="popover-title"></h3>'
								+ '<div class="popover-content" style="word-wrap: break-word; word-break: normal;"></div></div>',
							title: '详细',
							content: text
						});
						
						if (__handler) {
							$timeout.cancel(__handler);
						}
						
						__handler = $timeout(function(){
							$element.popover('hide');
							__handler = null;
						}, 3000);
					}));
				}
			}, 0);
		}
	};
}]).directive('paginationX', function(){
	return {
		restrict: 'A',
		require: '^stTable',
		scope: {
			pageSize: '=?'
		},
		template: '<nav>'
			+ '<ul class="pagination">'
			+ '<li ng-class="{disabled: currentPage == 1}"><a ng-click="selectPage(1)">First</a>'
			+ '</li><li ng-class="{disabled: currentPage == 1}"><a ng-click="selectPage(currentPage-1)">&lt;</a>'
			+ '</li><li><a><input type="text" style="width:30px; margin:-5px 0px;" value="{{currentPage}}"> of {{pagination.numberOfPages}}</a>'
			+ '</li><li ng-class="{disabled: currentPage == pagination.numberOfPages}"><a ng-click="selectPage(currentPage+1)">&gt;</a>'
			+ '</li><li ng-class="{disabled: currentPage == pagination.numberOfPages}"><a ng-click="selectPage(pagination.numberOfPages)">Last</a></li>'
			+ '</ul>'
			+ '</nav>',
//		controller: function($scope) {
//			console.log($scope);
//			$scope.pagination = {
//				currentPage: 1,
//				pageSize: 10,
//				numOfPages: 1
//			};
//			
//			function convert(pagination, reverse) {
//				if (reverse) {
//					return {
//						number: pagination.pageSize,
//						start: pagination.pageSize * (pagination.currentPage - 1),
//						numberOfPages: pagination.pageSize
//					};
//				} else {
//					return pagination = {
//						currentPage: pagination.start / pagination.number + 1,
//						pageSize: pagination.number,
//						numOfPages: pagination.numberOfPages
//					};
//				}
//			}
//			
//			$scope.nextPage = function() {
//				if ($scope.pagination.currentPage < $scope.pagination.pageSize) {
//					$scope.pagination.currentPage++;
//				}
//				$scope.doPagination();
//			};
//			
//			$scope.firstPage = function() {
//				$scope.pagination.currentPage = 1;
//				$scope.doPagination();
//			};
//			
//			$scope.lastPage = function() {
//				$scope.pagination.currentPage = $scope.pagination.pageSize;
//				$scope.doPagination();
//			};
//			
//			$scope.prevPage = function() {
//				if ($scope.pagination.currentPage > 1) {
//					$scope.pagination.currentPage--;
//				}
//				$scope.doPagination();
//			};
//			
//			$scope.selectPage = function(page) {
//				if (page <= $scope.pagination.numOfPages && page >= 1) {
//					$scope.pagination.currentPage = page;
//				}
//				$scope.doPagination();
//			};
//			
//			$scope.doPagination = function() {
//				// Convert the pagination obj to compatible with smart table.
//				var p = {pagination: convert(pagination, true), init: $scope.init};
//				
//				// Call the paginate function to take action.
//				$scope.paginate.call(this, p);
//				
//				// Convey the pagination changes back to directive.
//				$scope.pagination = convert(p, false);
//			};
//			
//			$scope.set = {
//				setNumOfPages: function(pages) {
//					$scope.pagination.numOfPages = pages;
//				},
//				
//				setPageSize: function(size) {
//					$scope.pagination.pageSize = size;
//				},
//				
//				setCurrentPage: function(page) {
//					$scope.pagination.currentPage = page;
//				}
//			};
//		},
		
		link: function($scope, $element, attrs, ctrl) {
			$element.find('input').on('keypress', function(e){
				if (e.which == 13) {
					$scope.$apply(function(){
						$scope.selectPage(parseInt($(e.target).val()));
					});
				}
			});
			
			$scope.selectPage = function (page) {
				if (page > 0 && page <= $scope.pagination.numberOfPages && page != $scope.currentPage) {
					$scope.currentPage = page;
					ctrl.slice((page - 1) * $scope.pagination.number, $scope.pagination.number);
				}
			};

			if (!$scope.pagination) {
				ctrl.slice(0, $scope.pageSize);
			}
			
			$scope.$watch(function(){
				return ctrl.tableState().pagination;
			}, function(){
				$scope.pagination = ctrl.tableState().pagination;
				if ($scope.pagination.numOfPages == 0) {
					$scope.currentPage = 0;
				} else {
					$scope.currentPage = $scope.pagination.start / $scope.pagination.number + 1;
				}
				
				$scope.$watch('pagination.start', function(newValue, oldValue) {
					console.log($scope.pagination.numberOfPages);
					if (newValue == 0 && $scope.pagination.numberOfPages == 0) {
						$scope.currentPage = 0;
					}
				});
			});
		}
	}
}).directive('confirmDialogX', [function() {
	return {
		restrict: 'E',
		scope: {
			id: '@id',
			title: '@title',
			content: '@content',
			action: '=action'
		},
		template: '<div class="modal fade" tabindex="-1" role="dialog">'
			+ '  <div class="modal-dialog">'
			+ '    <div class="modal-content">'
			+ '      <div class="modal-header">'
			+ '        <button type="button" class="close" data-dismiss="modal" aria-label="Close"><span aria-hidden="true">&times;</span></button>'
			+ '        <h4 class="modal-title">{{title}}</h4>'
			+ '      </div>'
			+ '      <div class="modal-body">'
			+ '        <div class="alert alert-danger alert-dismissible fade in" role="alert">'
			+ '            <button type="button" class="close" data-dismiss="alert" aria-label="Close"><span aria-hidden="true">×</span></button>'
			+ '            <p style="text-align: left;">{{content}}</p>'
			+ '        </div>'
			+ '      </div>'
			+ '      <div class="modal-footer">'
			+ '        <button type="button" class="btn btn-default" data-dismiss="modal">取消</button>'
			+ '        <button type="button" class="btn btn-default" data-dismiss="modal" ng-click="action(context)">确认</button>'
			+ '      </div>'
			+ '    </div>'
			+ '  </div>'
			+ '</div>',
		link: function($scope, $element, attrs) {
			$scope.$on('confirm', function(){
				if (arguments.length > 1 && arguments[1] == attrs['id']) {
					$element.find('.modal').modal();
					
					if (arguments.length > 2) {
						$scope.context = arguments[2];
					}
				}
			});
		}
	};
}]).directive('selectX', ['$resource', function($resource) {
	return {
		restrict: 'E',
		scope: {
			id: '@id',
			url: '@url',
			onChange: '=onChange',
			onSelect: '=onSelect',
			onUnselect: '=onUnselect',
			onValidate: '=onValidate',
			multiple: '@multiple',
			data: '=data',
			tags: '@tags',
			urlHandler: '=urlHandler'
		},
		template: '<select class="form-control" style="width: 100%"></select>',
		link: function($scope, $element, attrs) {
			var __data = null;
			var __dataResource = null;
			
			if ($scope.url) {
				var dataResource = $resource($scope.url, {}, {
					fetch: {
						url: $scope.url,
						isArray: false,
						method: 'GET'
					}
				});
				
				dataResource.fetch(function(result){
					__data = $scope.urlHandler.call(this, result);
		    		init();
		    	});
			} else {
				__data = $scope.data;
			}
			
			function init() { 
				if (!$scope.multiple && $scope.tags) {
					$scope.multiple = true;
					$scope.singleTagMode = true;
				}
				
				$element.find('select').select2({
					tags: $scope.tags,
					multiple: $scope.multiple,
					allowClear: true,
					data: __data
				});
				
				$element.find('select').on('change', function(e) {
					$scope.onChange && $scope.onChange.call(this, $(e.target).val());
				}).on('select2:select', function(e){
					// Single tags support.
					if ($scope.singleTagMode) {
						var selected = null;
						if ((selected = $(e.target).val()) && selected.length > 1) {
							$element.find('select').val(e.params.data.id).change();
						}
					}
					
					$scope.onSelect && $scope.onSelect.call(this, e.params.data.id);
					$scope.onValidate && $scope.onValidate.call(this, e.params.data.id, $element.find('select').val());
				}).on('select2:unselect', function(e){
					$scope.onUnselect && $scope.onUnselect.call(this, e.params.data.id);
				});
				
				// reset select2 control.
				$scope.$on('select2:clear', function(e, id){
					if (id != $scope.id) {
						return;
					}
					$element.find('select').val('').trigger('change');
				});
				
				$scope.$on('select2:init', function(e, id, data){
					if (id != $scope.id) {
						return;
					}

					if (data && data instanceof Array) {
						$element.find('select').val(data).trigger('change');;
					} 
				});
				
				$scope.$on('select2:data', function(e, id) {
					if (id != $scope.id) {
						return;
					}
					
					if ($scope.url) {
						dataResource.fetch(function(result){
							__data = $scope.urlHandler.call(this, result);
							// rebuild select control.
							$element.find('select').select2('destroy');
							$element.find('select').select2({
								tags: $scope.tags,
								multiple: $scope.multiple,
								allowClear: true,
								data: __data
							});
				    	});
					}
				});
			}
		}
	};
}]).directive('refreshX', ['$compile', '$templateCache', function($compile, $templateCache) {
	return {
		restrict: 'A',
		scope: {
			ngInclude: '=ngInclude',
			ngIf: "=ngIf"
		},
		link: function($scope, $element, attrs) {
			$scope.$on('refresh', function(){
				$element.html($templateCache.get($scope.ngInclude));
				$compile($element.contents())($scope.ngIf);
			});
		}
	};
}]).directive('popoverX', ['$compile', function($compile) {
	return {
		restrict: 'E',
		transclude: true,
		scope: {
			context: '=?',
			content: '@',
			title: '@',
			target: '@',
			placement: '@'
		},
		template: '<ng-transclude class="hide"></ng-transclude>',
		link: function($scope, $element, attrs, ctrl, $transclude) {
			var templateOffered = false;
			$element.find('ng-transclude').append($transclude($scope, function(clone, $scope) {
				if (clone.length > 0) {
					templateOffered = true;
				} 
			}));
			
			$($scope.target).on('click', function(e){
				var $target = $(e.target);
				if (!$target.data('created')) {
					e.stopPropagation();
					$target.popover({
						title: $scope.title,
						content: templateOffered? $element.find('ng-transclude').html(): $scope.content,
						html: templateOffered,
						placement: $scope.placement? $scope.placement: 'right'
					}).popover('show');
					
					$target.data('created', true);
				}
			});
		}
	};
}]);
