application_module.controller('app-topic-controller', [ '$scope', 'ApplicationService', '$location', function($scope, ApplicationService, $location) {
	console.log("app-topic-controller");
	$scope.new_application = {
		storageType : 'mysql',
		codecType : 'json',
		languageType : 'java'
	};
	$scope.productLines = ApplicationService.get_productLines();
	$scope.storageTypes = [ 'mysql', 'kafka' ];
	$scope.codecTypes = [ 'json', 'avro' ];
	$scope.languageTypes = [ 'java', '.net' ];
	$scope.example_request = {
		productLine : 'fx',
		entity : 'topic',
		event : 'created',
		codecType : 'json',
		maxMsgNumPerDay : 1000000,
		retentionDays : 7,
		size : 1,
		ownerName : '顾庆',
		ownerEmail : 'q_gu@ctrip.com',
		description : 'topic创建'
	}
	$scope.create_topic_application = function(new_app) {
		bootbox.confirm({
			title : "请确认",
			message : "确认要申请新建 Topic: <label class='label label-danger'>" + new_app.productLine + "." + new_app.entity + "." + new_app.event + "</label> 吗？",
			locale : "zh_CN",
			callback : function(result) {
				if (result) {
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
