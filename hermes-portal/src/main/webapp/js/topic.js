var show_op_info = function() {
	"use strict";

	var info_elem, hideHandler, that = {};
	that.init = function(options) {
		info_elem = $(options.selector);
	};
	that.show = function(text) {
		clearTimeout(hideHandler);
		info_elem.find("span").html(text);
		info_elem.delay(200).fadeIn().delay(4000).fadeOut();
	};
	return that;
}();

$(function() {
	show_op_info.init({
		"selector" : ".op-alert"
	});
});

function to_topic_rows(topics) {
	var rows = [];
	for (var i = 0; i < topics.length; i++) {
		var row = angular.copy(topics[i]);
		row.partitions = row.partitions.length;
		rows.push(row);
	}
	return rows;
}

function filter_topic_rows(rows, filter, table_state) {
	rows = table_state.search.predicateObject ? filter('filter')(rows, table_state.search.predicateObject) : rows;
	if (table_state.sort.predicate) {
		rows = filter('orderBy')(rows, table_state.sort.predicate, table_state.sort.reverse);
	}
	return rows;
}

function reload_table(scope, data) {
	scope.src_topics = data;
	scope.topic_rows = to_topic_rows(scope.src_topics);
}
angular.module('hermes-topic', [ 'ngResource', 'smart-table' ]).controller('topic-controller',
		[ '$scope', '$filter', '$resource', function(scope, filter, resource) {
			topic_resource = resource('/api/topics/:name', {}, {
				"remove" : {
					method : 'DELETE'
				}
			});

			scope.is_loading = true;
			scope.src_topics = [];
			scope.topic_rows = [];

			scope.get_topics = function get_topics(table_state) {
				topic_resource.query().$promise.then(function(query_result) {
					scope.src_topics = query_result;
					scope.topic_rows = filter_topic_rows(to_topic_rows(scope.src_topics), filter, table_state);
					scope.is_loading = false;
				});
			};

			scope.add_topic = function add_topic(new_topic) {
				topic_resource.save(new_topic).$promise.then(function(save_result) {
					topic_resource.query().$promise.then(function(query_result) {
						reload_table(scope, query_result);
						show_op_info.show("新增Topic成功, Topic名称：" + new_topic.name);
					});
				});
			};

			scope.del_topic = function del_topic(name) {
				bootbox.confirm("确认删除 Topic: " + name + "?", function(result) {
					if (result) {
						topic_resource.remove({
							"name" : name
						}).$promise.then(function(remove_result) {
							topic_resource.query().$promise.then(function(query_result) {
								reload_table(scope, query_result);
								show_op_info.show("删除Topic：" + name + "成功！");
							});
						});
					}
				});
			};
		} ]);
