topic_module.controller('kafka-add-controller', [ '$scope', '$resource', 'TopicService', function($scope, $resource, TopicService) {
	$scope.new_topic = {
		partitions : [ {} ],
		consumerRetryPolicy : '1:[3,6,9]',
		ackTimeoutSeconds : 5,
		endpointType : 'kafka',
		storageType : 'kafka',
		codecType : 'avro',
		properties : []
	};
	$scope.current_datasource_names = [];
	$scope.kafka_property_names = [ 'partitions', 'replication-factor', 'retention.bytes', 'retention.ms' ]
	$scope.endpoint_types = [ 'kafka', 'broker' ];
	$scope.codec_types = [ 'avro', 'json' ];

	var meta_resource = $resource('/api/meta/storages', {}, {
		'get_storage' : {
			method : 'GET',
			isArray : true,
		}
	});

	$scope.add_property = function() {
		$scope.new_topic.properties.push({
			name : $scope.kafka_property_names[0],
			value : ''
		});
	};

	$scope.delete_property = function(index) {
		$scope.new_topic.properties.splice(index, 1);
	}

	meta_resource.get_storage({
		'type' : 'kafka'
	}, function(data) {
		for (var i = 0; i < data[0].datasources.length; i++) {
			$scope.current_datasource_names.push(data[0].datasources[i].id);
		}
		for (var i = 0; i < $scope.new_topic.partitions.length; i++) {
			$scope.new_topic.partitions[i].readDatasource = $scope.current_datasource_names[1];
			$scope.new_topic.partitions[i].writeDatasource = $scope.current_datasource_names[0];
		}
	});

	$scope.add_partition = function() {
		var new_partition = {};
		new_partition.readDatasource = $scope.current_datasource_names[1];
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
						TopicService.save_topic($scope.new_topic, '/kafka');
					}
				}
			});
		}
	};
} ]);