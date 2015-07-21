topic_module.controller('list-controller', [ '$scope', '$resource', '$routeParams', 'TopicService', 'upload',
		function($scope, $resource, $routeParams, TopicService, upload) {
			$scope.cur_time = new Date();
			$scope.current_topic_type = $routeParams['type'];
			TopicService.fetch_topics($scope.current_topic_type);

			$scope.$watch(TopicService.get_topics, function() {
				$scope.current_topics = TopicService.get_topics();
			});

			$scope.delete_topic = function(topic, index) {
				bootbox.confirm({
					title : "请确认",
					message : "确认要删除 Topic: <label class='label label-danger'>" + topic.name + "</label> 吗？",
					locale : "zh_CN",
					callback : function(result) {
						if (result) {
							TopicService.delete_topic(topic, index);
						}
					}
				});
			};

			$scope.set_selected = function(row) {
				$scope.schema_data = {
					topicId : row.id,
					schema : "{name : '" + row.name + "',type:'avro',compatibility:'FORWARD'}"
				};
			};

			$scope.upload_result = function(row, success, response) {
				if (success) {
					show_op_info.show("上传schema成功!", true);
					row.schema.version = row.schema.version == undefined ? 1 : row.schema.version + 1;
					row.schema.name = row.name + '-value';
					row.schema.id += 1;
				} else {
					if (response.status == 409) {
						show_op_info.show("上传失败, Schema已经存在!", false);
					} else {
						show_op_info.show("上传失败: " + response.data, false);
					}
				}
			};

			$scope.sync_topic = function(row) {
				bootbox.confirm({
					title : "请确认",
					message : "确认要同步 Topic: <label class='label label-success'>" + row.name + "</label> 至下一环境吗？",
					locale : "zh_CN",
					callback : function(result) {
						if (result) {
							TopicService.sync_topic(row.name);
						}
					}
				});
			}
		} ]);