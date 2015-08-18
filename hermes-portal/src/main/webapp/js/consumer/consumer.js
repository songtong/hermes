function filter_consumer_rows(rows, filter, table_state) {
	rows = table_state.search.predicateObject ? filter('filter')(rows,
			table_state.search.predicateObject) : rows;
	if (table_state.sort.predicate) {
		rows = filter('orderBy')(rows, table_state.sort.predicate,
				table_state.sort.reverse);
	}
	return rows;
}

function reload_table(scope, data) {
	scope.src_consumers = data;
	scope.consumer_rows = scope.src_consumers;
}

angular.module('hermes-consumer', [ 'ngResource', 'smart-table' ]).controller(
		'consumer-controller',
		[
				'$scope',
				'$filter',
				'$resource',
				function(scope, filter, resource) {
					consumer_resource = resource(
							'/api/consumers/:topics/:consumer', {}, {});
					meta_resource = resource('/api/meta', {}, {
						'get_topic_names' : {
							method : 'GET',
							isArray : true,
							url : '/api/meta/topics/names'
						}
					});

					scope.is_loading = true;
					scope.src_consumers = [];
					scope.consumer_rows = [];
					scope.new_consumer = {
						orderedConsume : true,
						topicNames: []
					};

					scope.order_opts = [ true, false ];

					meta_resource.get_topic_names({}, function(result) {
						var result = new Bloodhound({
							local : result,
							datumTokenizer : Bloodhound.tokenizers.whitespace,
							queryTokenizer : Bloodhound.tokenizers.whitespace,
						});
						$('#inputTopicName').tokenfield({
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

					scope.get_consumers = function get_consumers(table_state) {
						consumer_resource.query().$promise.then(function(
								query_result) {
							scope.src_consumers = query_result;
							scope.consumer_rows = filter_consumer_rows(
									scope.src_consumers, filter, table_state);
							scope.is_loading = false;
						});
					};
					scope.newTopicNames = "";
					scope.add_consumer = function add_consumer(new_consumer) {
						new_consumer.topicNames = scope.newTopicNames.split(",");
						consumer_resource.save({
							topics : new_consumer.topicNames,
							consumer : new_consumer.groupName
						}, new_consumer, function(save_result) {
							console.log(save_result);
							consumer_resource.query().$promise.then(function(
									query_result) {
								reload_table(scope, query_result);
								show_op_info.show("新增成功: "
										+ new_consumer.groupName + "("
										+ new_consumer.topicNames + ")", true);
							});
						}, function(error_result) {
							show_op_info.show("新增失败: " + new_consumer.groupName
									+ "(" + new_consumer.topicNames + "), "
									+ error_result.data, false);
						});
					};

					scope.del_consumer = function del_consumer(topicNames,
							groupName) {
						bootbox.confirm("确认删除 Consumer: " + groupName + "("
								+ topicNames + ")?", function(result) {
							if (result) {
								consumer_resource.remove({
									topics : topicNames,
									consumer : groupName
								}, function(remove_result) {
									consumer_resource.query({},
											function(query_result) {
												reload_table(scope,
														query_result);
												show_op_info.show("删除成功: "
														+ groupName + "("
														+ topicNames + ")",
														true);
											});
								}, function(error_result) {
									show_op_info.show("删除失败: " + groupName
											+ "(" + topicNames + "), "
											+ error_result.data, false);
								});
							}
						});
					};
				} ]);
