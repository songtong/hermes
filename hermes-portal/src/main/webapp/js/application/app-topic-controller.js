application_module.controller('app-topic-controller', [ '$scope', 'ApplicationService', '$location', function($scope, ApplicationService, $location) {
	console.log("app-topic-controller");
	console.log(ssoUser);
	console.log(ssoMail);
	$scope.new_application = {
		storageType : 'mysql',
		codecType : 'json',
		languageType : 'java',
		ownerName1 : ssoUser,
		ownerEmail1Prefix : ssoMail.split('@')[0]
	};
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
	$scope.storageTypes = [ 'mysql', 'kafka' ];
	$scope.codecTypes = [ 'json', 'avro' ];
	$scope.languageTypes = [ 'java', '.net' ];
	$scope.create_topic_application = function(new_app) {
		bootbox.confirm({
			title : "请确认",
			message : "确认要申请新建 Topic: <label class='label label-danger'>" + new_app.productLine + "." + new_app.entity + "." + new_app.event + "</label> 吗？",
			locale : "zh_CN",
			callback : function(result) {
				if (result) {
					new_app.ownerEmail1 = new_app.ownerEmail1Prefix + "@Ctrip.com";
					new_app.ownerEmail2 = new_app.ownerEmail2Prefix + "@Ctrip.com";
					console.log(new_app);
					ApplicationService.create_topic_application(new_app).then(function(result) {
						show_op_info.show("申请添加Topic: <" + result.productLine + "." + result.entity + "." + result.event + "> 成功！", true);
						console.log(result);
						$location.path("review/" + result.id);
					}, function(result) {
						console.log(result);
						show_op_info.show("申请添加Topic失败，原因：" + result.data, false);
					});
				}
			}
		});
	}

} ])
