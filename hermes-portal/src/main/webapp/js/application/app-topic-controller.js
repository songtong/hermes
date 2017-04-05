application_module.controller('app-topic-controller', [
		'$scope',
		'ApplicationService',
		'$location',
		'$resource',
		'$window',
		function($scope, ApplicationService, $location, $resource, $window) {
			var idcResource = $resource('/api/idcs/primary', {}, {})
			$scope.new_application = {
				storageType : 'mysql',
				baseCodecType : 'json',
				languageType : 'java',
				ownerName1 : ssoUser,
				ownerEmail1Prefix : ssoMail.split('@')[0],
				needCompress : 'true',
				compressionType : 'deflater',
				compressionLevel : 1,
				priorityMessageEnabled : false,
				retentionDays : 3,
				writePrimaryIdc : true,
				needInOrder : true
			};

			$scope.kafkaFullDrEnabled = false;
			ApplicationService.get_kafka_fulldr_enabled().then(function(result) {
				$scope.kafkaFullDrEnabled = result.value;
			})

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
			$scope.compressionTypes = [ 'gzip', 'deflater' ];
			$scope.compressionLevels = [ {
				key : '低',
				value : 1
			}, {
				key : '中',
				value : 5
			}, {
				key : '高',
				value : 9
			} ];

			$scope.needMultiIdc = [ true, false ];
			$scope.primaryIdc = null;

			idcResource.get(function(result) {
				$scope.primaryIdc = result;
			})

			$scope.create_topic_application = function(new_app) {
				var message = "申请新建 Topic: <label class='label label-danger'>" + new_app.productLine + "." + new_app.entity + "." + new_app.event + "</label> ";
				if (new_app.storageType == 'kafka' && $scope.kafkaFullDrEnabled) {
					if (new_app.writePrimaryIdc) {
						message = message + "<div style='margin-top: 5px;'><strong class='text-danger'>您选择写Kafka主机房, 当前主机房为：" + $scope.primaryIdc.name + "( " + $scope.primaryIdc.displayName + " )，推荐在主机房部署生产者。并需要升级客户端至0.8.0以上版本。</strong></div>";
					} else {
						message = message + "<div style='margin-top: 5px;'><strong class='text-danger'>您选择写生产者同机房Kafka集群，需要该Topic的消费者全部双机房部署，否则会有消息丢失！并需要升级客户端至0.8.0。</strong></div>";
					}

				}
				bootbox.confirm({
					title : "请确认",
					message : message,
					locale : "zh_CN",
					callback : function(result) {
						if (result) {
							if (new_app.ownerEmail1Prefix)
								while (new_app.ownerEmail1Prefix.indexOf('@') != -1)
									new_app.ownerEmail1Prefix = new_app.ownerEmail1Prefix.split("@")[0];
							if (new_app.ownerEmail2Prefix)
								while (new_app.ownerEmail2Prefix.indexOf('@') != -1)
									new_app.ownerEmail2Prefix = new_app.ownerEmail2Prefix.split("@")[0];
							new_app.ownerEmail1 = new_app.ownerEmail1Prefix + "@Ctrip.com";
							new_app.ownerEmail2 = new_app.ownerEmail2Prefix + "@Ctrip.com";
							new_app.codecType = new_app.baseCodecType;
							if (new_app.needCompress == 'true') {
								new_app.codecType = new_app.codecType + ',' + new_app.compressionType;
								if (new_app.compressionType == 'deflater') {
									new_app.codecType = new_app.codecType + '(' + new_app.compressionLevel + ')';
								}
							}
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

			$scope.checkQmqRouter = function checkQmqRouter() {
				if (!$scope.new_application.needInOrder && $scope.new_application.storageType == 'mysql' && $scope.new_application.languageType == 'java') {
					bootbox.alert({
						title : "<span class='glyphicon glyphicon-alert' style='color:#a94442;'></span><span style='font-family: 微软雅黑;color:#a94442;'>&emsp;请注意</span>",
						message : "<p>根据公司最新Java规范，无序消息队列请使用QMQ: <br />" + "<br>如何开始: <a href='http://conf.ctripcorp.com/pages/viewpage.action?pageId=118310160'>QMQ 携程版开发手册</a>"
								+ "<br />治理界面 : <a href='http://qmq.fws.qa.nt.ctripcorp.com/application/create.do' target='_blank'>FWS 环境</a>; <a href='http://qmq.uat.qa.nt.ctripcorp.com/application/create.do' target='_blank'>UAT 环境</a>; <a href='http://qmq.ctripcorp.com/application/create.do' target='_blank'>PROD 环境</a> "
								+ "<br><br /> <span class='text-warning'><b>如仍选择使用Hermes，请继续完善表单。</b></span></p>",
						buttons : {
							ok : {
								label : '我已知晓',
								className : 'btn-danger pull-right'
							}
						}

					})
				}
			}

		} ])
