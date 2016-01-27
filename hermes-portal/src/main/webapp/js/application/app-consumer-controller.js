application_module.controller('app-consumer-controller', [ '$scope', 'ApplicationService', '$location', function($scope, ApplicationService, $location) {
	$scope.topicStorageType = "mysql";
	$scope.productLines = ApplicationService.get_productLines();
	$scope.getFilteredProductLine = function(val) {
		var result = [];
		for (var idx = 0; idx < $scope.productLines.length; idx++) {
			if ($scope.productLines[idx].indexOf(val) > -1) {
				result.push($scope.productLines[idx]);
			}
		}
		return result;
	};
	ApplicationService.get_topic_names().then(function(result) {
		var result = new Bloodhound({
			local : result,
			datumTokenizer : Bloodhound.tokenizers.whitespace,
			queryTokenizer : Bloodhound.tokenizers.whitespace,
		});
		$('#inputTopicName').on(('tokenfield:createdtoken'), function(e) {
			var topicList = $('#inputTopicName').tokenfield('getTokens');
			var idx;
			for (idx = 0; idx < (topicList.length - 1); idx++) {
				if (e.attrs.value == topicList[idx].value) {
					topicList.pop();
					$('#inputTopicName').tokenfield('setTokens', topicList);
				}
			}
			for (idx = 0; idx < result.local.length; idx++) {
				if (e.attrs.value == result.local[idx]) {
					break;
				}
			}
			if (idx == result.local.length) {
				topicList.pop();
				$('#inputTopicName').tokenfield('setTokens', topicList);
			}
		}).tokenfield({
			typeahead : [ {
				hint : true,
				highlight : true,
				minLength : 1
			}, {
				name : 'topics',
				source : result
			} ],
			beautify : false
		});
	});

	$scope.create_consumer_application = function(new_consumer) {
		bootbox.confirm({
			title : "请确认",
			message : "确认要申请新建consumer: <label class='label label-danger'>" + new_consumer.productLine + "." + new_consumer.product + "." + new_consumer.project + "</label> 吗？",
			locale : "zh_CN",
			callback : function(result) {
				if (result) {
					new_consumer.ownerEmail1 = new_consumer.ownerEmail1 + "@Ctrip.com";
					new_consumer.ownerEmail2 = new_consumer.ownerEmail2 + "@Ctrip.com";
					console.log(new_consumer);
					ApplicationService.create_consumer_application(new_consumer).then(function(result) {
						show_op_info.show("申请提交成功！", true);
						console.log(result);
						$location.path("review/" + result.id);
					}, function(result) {
						console.log(result);
						show_op_info.show("申请提交失败，原因：" + result.data, false);
					});
				}
			}
		});
	}
} ])
