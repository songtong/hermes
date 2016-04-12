angular.module('hermes-consumer', [ 'ngResource', 'smart-table', 'xeditable', 'utils', 'components']).run(function(editableOptions) {  
	editableOptions.theme = 'bs3';
}).controller('consumer-controller', ['$scope', '$filter', '$resource', 'clone', function(scope, filter, resource, clone) {
	consumer_resource = resource('/api/consumers/:topic/:consumer', {}, {
			'add_consumer' : {
				method:'POST',
				url:'/api/consumers/'
			},
			'update_consumer' : {
				method:'PUT',
				url:'/api/consumers/'
			}
	});
	
	meta_resource = resource('/api/', {}, {
		'get_topic_names' : {
			method : 'GET',
			isArray : true,
			url : '/api/topics/names'
		}
	});
		
	consumer_resource.query().$promise.then(function(result) {
		scope.consumers = result;
		scope.$broadcast('initialized');
	});
	
	scope.newConsumer = {
		orderedConsume : true
	};
	
	scope.resetOption = null;

	scope.newTopicNames = "";
	
	scope.order_opts = [ true, false ];
	
	scope.currentTimestamp = new Date();
	
	scope.maxTimestamp = scope.currentTimestamp.format('%y-%M-%dT%H:%m:%s');
	
	scope.currentTimestamp.setMilliseconds(0);
	scope.currentTimestamp.setSeconds(0);
	
	meta_resource.get_topic_names({}, function(result) {
		var result = new Bloodhound({
			local : result,
			datumTokenizer : Bloodhound.tokenizers.whitespace,
			queryTokenizer : Bloodhound.tokenizers.whitespace,
		});
		
		$('#inputTopicName').on(('tokenfield:createdtoken'),function(e){
			var topicList = $('#inputTopicName').tokenfield('getTokens');
			var idx;
			for ( idx =0;idx<(topicList.length-1); idx++){
				if(e.attrs.value == topicList[idx].value){
					topicList.pop();
					$('#inputTopicName').tokenfield('setTokens', topicList);
				}
			}
			for( idx =0;idx< result.local.length;idx++){
				if(e.attrs.value == result.local[idx]){
					break;
				}
			}
			if(idx==result.local.length){
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
	
	
	scope.setCurrentConsumer = function(consumer) {
		scope.currentConsumer = consumer;
		scope.currentConsumer.resetOption = 'latest';
	};
	
	scope.submitReset = function() {
		if (scope.currentConsumer.resetOption == 'timepoint') {
			scope.currentConsumer.timestamp = scope.currentTimestamp;
		}
		console.log(scope.currentConsumer);
	}
	
	scope.expandCollapse = function($event) {
		var $target = $(event.currentTarget);
		scope.currentConsumer.resetOption = $target.find('input[type=radio]').val();
		if (scope.currentConsumer.resetOption == 'timepoint') {
			scope.maxTimestamp = new Date().format('%y-%M-%dT%H:%m:%s');
			$('#timepoint').removeClass('collapse');
		} else {
			$('#timepoint').addClass('collapse');
		}
	}

	scope.addConsumer = function(newConsumer) {
		var topics = scope.newTopicNames.split(",");
		for(var i=0 ;i<topics.length;i++){
			var consumer = clone(newConsumer);
			consumer.topicName = topics[i];
			
			(function(consumer) {
				consumer_resource.add_consumer({}, consumer, function(result) {
					consumer_resource.query().$promise.then(function(result) {
						this.consumers = result
						show_op_info.show("新增 consumer "
								+ consumer.name + " for topic ("
								+ consumer.topicName+ ") 成功!", true);
					});
				}, function(result) {
					show_op_info.show("新增 consumer " + consumer.name
							+ " for topics (" + consumer.topicName + ") 失败! "
							+ result.data, false);
				});
			})(consumer);
		}
	};
	
	scope.update_consumer = function update_consumer(data,topicName,name){
		data.name=name;
		data.topicName=topicName;
		consumer_resource.update_consumer({}, data, function(save_result) {
			consumer_resource.query().$promise.then(function(result) {
				scope.consumers = result;
				show_op_info.show("修改 consumer "
						+ data.name + " for topic ("
						+ data.topicName + ") 成功!", true);
			});
		}, function(result) {
			show_op_info.show("修改 consumer " + data.name
					+ " for topic (" + data.topicName + ") 失败! "
					+ result.data, false);
		});
		
	};
	
	scope.confirmDeletion = function(topicName, name) {
		scope.consumerName = name;
		scope.$broadcast('confirm', 'deletionConfirm', {topic: topicName, consumer: name});
	};

	scope.deleteConsumer = function(context) {
		consumer_resource.remove({
			topic : context.topic,
			consumer : context.consumer
		}, function(remove_result) {
			consumer_resource.query({}, function(result) {
						scope.consumers = result;
						show_op_info.show("删除成功: "
								+ name + "("
								+ topicName + ")",
								true);
					});
		}, function(result) {
			show_op_info.show("删除失败: " + context.consumer
					+ "(" + context.topic + "), "
					+ result.data, false);
		});
	};
}]);
