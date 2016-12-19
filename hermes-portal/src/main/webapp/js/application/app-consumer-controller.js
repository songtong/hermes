application_module.controller('app-consumer-controller', [ '$scope', 'ApplicationService', '$location', '$resource', function($scope, ApplicationService, $location, $resource) {
	var idcResource = $resource('/api/idcs/primary', {}, {})
	$scope.primaryIdc = null;
	idcResource.get(function(result) {
		$scope.primaryIdc = result;
	})

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
	$scope.new_application = {
		ownerName1 : ssoUser,
		ownerEmail1Prefix : ssoMail.split('@')[0],
		ackTimeoutSeconds : 5,
		needRetry : 'true',
		enabled : true,
		needMultiIdc : true,
		retryCount : 3,
		retryInterval : 3

	};

	$scope.kafkaFullDrEnabled = false;
	ApplicationService.get_kafka_fulldr_enabled().then(function(result) {
		$scope.kafkaFullDrEnabled = result.value;
		if (!$scope.kafkaFullDrEnabled) {
			$scope.new_application.needMultiIdc = false;
		}
	})

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
		var message = "申请新建consumer: <label class='label label-danger'>" + new_consumer.productLine + "." + new_consumer.product + "." + new_consumer.project + "</label> ";
		if ($scope.kafkaFullDrEnabled) {
			message = message + "<div class='text-danger' style='margin-top:5px'><strong>如果您的Topic为Kafka类型，请注意：</strong><div>";
			if (new_consumer.needMultiIdc) {
				message = message + "您选择了需要多机房容灾，这要求您必须在所有机房都部署消费者，否则未部署消费者机房的Kafka集群消息将会丢失。并且请升级客户端版本至0.8.0以上版本。";
			} else {
				message = message + "您选择了不需要多机房容灾，消费者只会消费主机房Kafka集群消息；当前主机房为：" + $scope.primaryIdc.name + "（ " + $scope.primaryIdc.displayName + "），生产者发往其他机房的消息将会丢失！并且请升级客户端版本至0.8.0。";
			}
		}

		bootbox.confirm({
			title : "<strong>请确认</strong>",
			message : message,
			locale : "zh_CN",
			callback : function(result) {
				if (result) {
					new_consumer.ownerEmail1 = new_consumer.ownerEmail1Prefix + "@Ctrip.com";
					new_consumer.ownerEmail2 = new_consumer.ownerEmail2Prefix + "@Ctrip.com";
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
