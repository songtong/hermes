topic_module.controller('mysql-add-controller', [ '$scope', '$resource', 'TopicService', function($scope, $resource, TopicService) {
	$scope.new_topic = {
		partitions : [ {} ],
		consumerRetryPolicy : '1:[3,6,9]',
		ackTimeoutSeconds : 5,
		endpointType : 'broker',
		storageType : 'mysql',
		codecType : 'json',
		storagePartitionSize : '1000000',
		resendPartitionSize : '5000',
		storagePartitionCount : '10'
	};
	$scope.codec_types = [ 'json', 'cmessaging' ];
	$scope.current_datasource_names = [];

	var meta_resource = $resource('/api/meta/storages', {}, {
		'get_storage' : {
			method : 'GET',
			isArray : true,
		}
	});

	meta_resource.get_storage({
		'type' : 'mysql'
	}, function(data) {
		for (var i = 0; i < data[0].datasources.length; i++) {
			$scope.current_datasource_names.push(data[0].datasources[i].id);
		}
		for (var i = 0; i < $scope.new_topic.partitions.length; i++) {
			$scope.new_topic.partitions[i].readDatasource = $scope.current_datasource_names[0];
			$scope.new_topic.partitions[i].writeDatasource = $scope.current_datasource_names[0];
		}
	});

	$scope.add_partition = function() {
		var new_partition = {};
		new_partition.readDatasource = $scope.current_datasource_names[0];
		new_partition.writeDatasource = $scope.current_datasource_names[0];
		$scope.new_topic.partitions.push(new_partition);
	};

	$scope.delete_partition = function(index) {
		$scope.new_topic.partitions.splice(index, 1);
	};

	$scope.save_topic = function() {
		if ($scope.new_topic.name == undefined) {
			show_op_info.show("Topic 名称不能为空", false);
		} else {
			bootbox.confirm({
				title : "请确认",
				message : "确认要增加 Topic: <label class='label label-success'>" + $scope.new_topic.name + "</label> 吗？",
				locale : "zh_CN",
				callback : function(result) {
					if (result) {
						TopicService.save_topic($scope.new_topic, '/mysql');
					}
				}
			});
		}
	};
} ]);