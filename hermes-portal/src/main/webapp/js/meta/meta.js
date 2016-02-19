global = {};

var meta_app = angular.module('hermes-meta', [ 'ngResource' ]);
meta_app.controller('meta-controller', [ '$scope', '$resource', '$q', function($scope, $resource, $q) {
	var meta_resource = $resource('/api/meta', {}, {
		'build_meta' : {
			method : 'POST',
			url : '/api/meta/build'
		},
		'preview_meta' : {
			method : 'GET',
			url : '/api/meta/preview'
		}
	})

	$scope.metaStatus = "oldMeta";
	$scope.meta = {
		oldMeta : "old",
		newMeta : "new",
		diff : ""
	}

	$scope.refreshCachedMeta = function refreshCachedMeta() {
		meta_resource.get({}, function(result) {
			$scope.meta.oldMeta = JSON.stringify(result, null, "\t");
			meta_resource.preview_meta({}, function(result) {
				$scope.meta.newMeta = JSON.stringify(result, null, "\t");
				var options = {
					source : $scope.meta.oldMeta,
					diff : $scope.meta.newMeta,
					mode : "diff", // beautify, diff, minify, parse
					lang : "javascript",
					diffview : "sidebyside",
					objsort : true,
					context : 5
				// number of indent characters per indent
				}
				var pd = prettydiff(options); // returns and array:
				// [beautified, report]
				$scope.meta['diff'] = strToDivDom(pd[0]).getElementsByClassName('diff');
				$("#prettydiff").empty();
				$("#prettydiff").append($scope.meta['diff']);
			})
		})
	}
	$scope.refreshCachedMeta();
	$scope.buildMeta = function buildMeta() {
		bootbox.confirm({
			title : "请确认",
			message : "确认要build meta吗",
			locale : "zh_CN",
			callback : function(result) {
				if (result) {
					meta_resource.build_meta({}, function(result) {
						show_op_info.show("Build meta success!", true);
						$scope.refreshCachedMeta();
					}, function(result) {
						show_op_info.show("Build meta failed: " + result.data, false);
					})
				}
			}
		});
		
	}
	$scope.previewMeta = function previewMeta() {
		meta_resource.preview_meta({}, function(result) {
			$scope.meta.newMeta = result;
		}, function(result) {
			show_op_info.show("Preview mate failed: " + result.data, false)
		})
	}
} ])