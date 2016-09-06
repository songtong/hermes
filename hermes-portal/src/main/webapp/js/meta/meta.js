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
		},
		'get_meta_diff' : {
			method : 'GET',
			url : '/api/meta/diff'
		}
	})

	$scope.metaStatus = "diff";
	$scope.meta = {
		oldMeta : "old",
		newMeta : "new",
		diff : ""
	}

	$scope.currentDiffDetail = "";

	$scope.metaDiff = {}

	$scope.reloadFinished = false;

	$scope.refreshCachedMeta = function refreshCachedMeta() {
		$("#prettydiff").empty();
		$scope.reloadFinished = false;
		$scope.currentDiffDetail = "";
		meta_resource.get({}, function(result) {
			$scope.meta.oldMeta = result;
		})
		meta_resource.preview_meta({}, function(result) {
			$scope.meta.newMeta = result;
		})
		meta_resource.get_meta_diff(function(result) {
			$scope.metaDiff = result;
			$scope.reloadFinished = true;
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

	$scope.toJsonFormat = function toJsonFormat(str) {
		return JSON.stringify(str, null, "\t");
	}

	$scope.prettyDiff = function prettyDiff(oldOne, newOne, diffDetail) {
		$scope.currentDiffDetail = diffDetail;
		var options = {
			source : JSON.stringify(oldOne, null, "\t"),
			diff : JSON.stringify(newOne, null, "\t"),
			mode : "diff", // beautify, diff, minify, parse
			lang : "javascript",
			diffview : "sidebyside",
			objsort : true,
			context : 10
		// number of indent characters per indent
		}
		var pd = prettydiff(options); // returns and array:
		// [beautified, report]
		$scope.meta['diff'] = strToDivDom(pd[0]).getElementsByClassName('diff');
		$("#prettydiff").empty();
		$("#prettydiff").append($scope.meta['diff']);
	}
} ])