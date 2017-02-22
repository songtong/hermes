topic_module.controller('list-controller', [ '$scope', '$resource', '$routeParams', 'TopicService', 'upload', 'user', '$filter', function($scope, $resource, $routeParams, TopicService, upload, user, $filter) {
	$scope.logined = user.admin;
	$scope.environment = environment;
	$scope.routeParams = $routeParams;
	$scope.cur_time = new Date();
	$scope.current_topic_type = $scope.routeParams['type'] == undefined ? 'mysql' : $scope.routeParams['type'];
	$scope.repositoryTypes = [ 'snapshots', 'releases' ];
	$scope.display_current_topics = [];
	$scope.pageSize = 15;

	$scope.getTopics = function getTopics(tableState) {
		// $scope.is_loading = true;
		console.log(tableState)
		var pagination = tableState.pagination;

		var startPage = (pagination.start || 0) / $scope.pageSize;
		var filter = tableState.search.predicateObject;
		TopicService.fetch_topics($scope.current_topic_type, filter, startPage, $scope.pageSize).then(function(result) {
			var topics = result.value;
			topics = tableState.search.predicateObject ? $filter('filter')(topics, tableState.search.predicateObject) : topics;
			if (tableState.sort.predicate) {
				topics = $filter('orderBy')(topics, tableState.sort.predicate, tableState.sort.reverse);
			}
			$scope.display_current_topics = topics;
			tableState.pagination.numberOfPages = Math.ceil(result.key / $scope.pageSize);
			$scope.is_loading = false;
		}, function(result) {
			$scope.display_current_topics = [];
			$scope.is_loading = false;
		});
	}

	$scope.delete_topic = function(topic, index) {
		console.log(topic, index)
		bootbox.confirm({
			title : "请确认",
			message : "确认要删除 Topic: <label class='label label-danger'>" + topic.name + "</label> 吗？",
			locale : "zh_CN",
			callback : function(result) {
				if (result) {
					if (topic.storageType == 'kafka') {
						TopicService.undeploy_topic(topic).then(function(result) {
							show_op_info.show("删除Kafka记录成功, 正在删除Topic...", true);
							$scope.delete_topic_in_meta(topic);
						}, function(result) {
							bootbox.confirm({
								title : "请确认",
								message : "删除Kafka记录失败，是否继续删除Topic?",
								locale : "zh_CN",
								callback : function(result) {
									if (result) {
										$scope.delete_topic_in_meta(topic, index);
									}
								}
							});
						})
					} else {
						$scope.delete_topic_in_meta(topic, index);
					}
				}
			}
		});
	};

	$scope.delete_topic_in_meta = function(topic, index) {
		TopicService.delete_topic(topic).then(function(result) {
			show_op_info.show("删除Topic成功！", true);
			$scope.display_current_topics.splice(index, 1);
		}, function(result) {
			show_op_info.show("删除Topic失败！", false);
		});
	}

	$scope.delete_schema = function(row) {
		console.log(row)
		bootbox.confirm({
			title : "请确认",
			message : "确认要删除 Schema: <label class='label label-danger'>" + row.schema.name + "</label> 吗？",
			locale : "zh_CN",
			callback : function(result) {
				if (result) {
					TopicService.delete_schema(row.schemaId).then(function(result) {
						show_op_info.show("删除schema成功!", true);
						TopicService.fetch_topics($scope.current_topic_type).then(function(result) {
							$scope.display_current_topics = result.value;
						}, function(result) {
							$scope.display_current_topics = [];
						});
					}, function(result) {
						show_op_info.show("删除schema失败:" + result.data, false);
					});
				}
			}
		});
	};

	$scope.set_selected = function(row) {
		$scope.schema_data = {
			topicId : row.id,
			schema : "{name : '" + row.name + "',type:'avro',compatibility:'FORWARD'}",
			userName : ssoUser,
			userMail : ssoMail
		};
	};

	$scope.newSchema = {};
	$scope.deploy_maven_row_selected = function(row) {
		$scope.newSchema.id = row.schemaId;
		$scope.newSchema.version = row.schema.version;
		console.log($scope.newSchema);
	}
	$scope.set_current_dependencyString = function(dependencyString) {
		$scope.current_dependencyString = dependencyString;
	}

	$scope.upload_result = function(row, success, response) {
		if (success) {
			show_op_info.show("上传schema成功!", true);
			TopicService.fetch_topics($scope.current_topic_type).then(function(result) {
				$scope.display_current_topics = result.value;
			}, function(result) {
				$scope.display_current_topics = [];
			});
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
					TopicService.sync_schema(row.name, ssoUser, ssoMail);
				}
			}
		});
	}

	$scope.deploy_schema_to_maven = function(row) {
		console.log($scope.newSchema);
		bootbox.confirm({
			title : "请确认",
			message : "确认要部署Schema至Maven吗？",
			locale : "zh_CN",
			callback : function(result) {
				if (result) {
					$scope.prompt = "正在部署中...";
					TopicService.deploy_schema_to_maven($scope.newSchema.id, $scope.newSchema.groupId, $scope.newSchema.artifactId, $scope.newSchema.version, $scope.newSchema.repositoryId).then(function(result) {
						$scope.prompt = "部署成功！";
						TopicService.fetch_topics($scope.current_topic_type).then(function(result) {
							$scope.display_current_topics = result.value;
						}, function(result) {
							$scope.display_current_topics = [];
						});
					}, function(result) {
						$scope.prompt = "部署失败！";
					});
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
		sort_partitions : function(value) {
			console.log(value);
			return value.partitions.length;
		}
	}
} ]);