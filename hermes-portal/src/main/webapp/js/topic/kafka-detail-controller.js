topic_module.run(function(editableOptions) {
	editableOptions.theme = 'bs3';
}).controller('kafka-detail-controller', [ '$scope', '$resource' ,'$routeParams','TopicService', '$q',function(scope, resource, routeParams, TopicService,$q) {
	
	scope.current_topic_type = routeParams['type'];
	scope.topic_name = routeParams['topicName'];
	
	scope.codec_types = [ 'avro', 'json' ];
	scope.endpoint_types = [ 'kafka', 'broker' ];
	
	scope.topic = TopicService.fetch_topic_detail(scope.topic_name).then(function(result){
		scope.topic=result;
	});
	scope.consumers = TopicService.fetch_consumers_for_topic(scope.topic_name).then(function(result){
		scope.consumers = result;
	});
	
	scope.update_topic = function(data){
		bootbox.confirm({
			title : "请确认",
			message : "确认要修改 Topic: <label class='label label-success'>" + scope.topic_name + "</label> 吗？",
			locale : "zh_CN",
			callback : function(result) {
				if (result) {
					TopicService.update_topic(scope.topic_name, data).then(function(result){
						show_op_info.show("修改topic ( "+ scope.topic_name + " ) 成功!", true);
						scope.topic = result;
					},function(data){
						show_op_info.show("修改topic ( "	+ scope.topic_name + " ) 失败!"+ data, false);
					});
				}else{
					scope.topic = TopicService.fetch_topic_detail(scope.topic_name).then(function(result){
						scope.topic=result;
					});
				}
			}
		});
	}
	
}]);
