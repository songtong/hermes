topic_module.controller('list-controller', [ '$scope', '$resource', '$routeParams', 'TopicService', 'upload',
		function($scope, $resource, $routeParams, TopicService, upload) {
			$scope.routeParams = $routeParams;
			$scope.cur_time = new Date();
			$scope.current_topic_type = $scope.routeParams['type'] == undefined ? 'mysql' : $scope.routeParams['type'];

			TopicService.fetch_topics($scope.current_topic_type);

			$scope.$watch(TopicService.get_topics, function() {
				$scope.current_topics = TopicService.get_topics();
			});
			$scope.display_current_topics = [].concat($scope.current_topics);
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
					row.schema = row.schema == null || row.schema == undefined ? {} : row_schema;
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
							TopicService.sync_topic(row.name, false);
						}
					}
				});
			};

			$scope.sync_schema = function(row) {
				bootbox.confirm({
					title : "请确认",
					message : "确认要同步Schema至下一环境吗？",
					locale : "zh_CN",
					callback : function(result) {
						if (result) {
							TopicService.sync_topic(row.name, true);
						}
					}
				});
			}

			function format_fields(fields) {
				var data = [];
				for ( var idx in fields) {
					var field_name = fields[idx].name;
					console.log(format_tree(fields[idx]));
					data.push({
						text : field_name,
						nodes : format_tree(fields[idx])
					})
				}
				return data;
			}

			function format_type(type_value) {
				if (Object.prototype.toString.call(type_value) === '[object Array]') {
					var r_type = type_value[0];
					if (r_type.name == undefined) {
						return [ {
							text : r_type
						} ];
					} else {
						return [ {
							text : r_type.name,
							nodes : format_tree(r_type)
						} ]
					}
				}
				return [ {
					text : type_value
				} ];
			}

			function format_tree(obj) {
				var data = [];
				if (obj instanceof Object) {
					for ( var key in obj) {
						var node = {};
						var value = obj[key];
						node.text = key;
						if (key == "fields") {
							node.nodes = format_fields(value);
						} else if (key == "type") {
							node.nodes = format_type(value);
						} else if (value instanceof Object) {
							node.nodes = format_tree(value);
						} else {
							node.nodes = [ {
								text : value
							} ];
						}
						data.push(node);
					}
				} else {
					data.push({
						text : obj
					});
				}
				return data;
			}

			$scope.show_schema_view = function(schema) {
				$scope.current_schema = schema;
				json_str = schema.schemaPreview.replace(/\\"/g, '"');
				if (json_str[0] == '"' && json_str[json_str.length - 1] == '"') {
					json_str = json_str.substring(1, json_str.length - 1);
				}
				var obj = JSON.parse(json_str);
				$scope.current_attr_json = obj;
				$("#data-tree").treeview({
					data : format_tree(obj),
					levels : 1
				});
				$("#schema-view").modal('show');
			};
			
			$scope.getters = {
					sort_partitions: function (value) {
						console.log(value);
			            return value.partitions.length;
			        }
			}
		} ]);