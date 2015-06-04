<%@ page contentType="text/html; charset=utf-8" isELIgnored="false" trimDirectiveWhitespaces="true"%>
<%@ taglib prefix="a" uri="/WEB-INF/app.tld"%>


<jsp:useBean id="ctx" type="com.ctrip.hermes.portal.console.topic.Context" scope="request" />
<jsp:useBean id="payload" type="com.ctrip.hermes.portal.console.topic.Payload" scope="request" />
<jsp:useBean id="model" type="com.ctrip.hermes.portal.console.topic.Model" scope="request" />

<a:layout>
	<div class="op-alert alert alert-info" role="alert" style="display: none;">
		<span>The examples populate this alert with dummy content</span>
	</div>
	<div ng-app="hermes-topic" ng-controller="topic-controller">
		<div class="panel panel-info">
			<div class="panel-heading">Hermes 消息主题列表</div>
			<table class="table table-hover" st-pipe="get_topics" st-table="topic_table">
				<thead>
					<tr>
						<th st-sort="name">Topic名称</th>
						<th st-sort="codecType" width="100px" style="text-align: center;">编码</th>
						<th st-sort="storageType" width="100px" style="text-align: center;">存储</th>
						<th st-sort="partitions" width="60px" style="text-align: center;">分区</th>
						<th st-sort="consumerRetryPolicy">消费重试策略</th>
						<th st-sort="ackTimeoutSeconds">ACK超时(秒)</th>
						<th st-sort="endpointType">Endpoint</th>
						<th style="text-align: left;"><button type="button" data-toggle="modal" data-target="#add-topic-modal" class="btn btn-xs btn-success" style="text-align: center;">新增</button></th>
					</tr>
					<tr>
						<th><input st-search="name" placeholder="Topic" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
						<th><input st-search="codecType" placeholder="Codec" class="input-sm form-control" type="search" style="text-align: center;" ng-model-options="{updateOn:'blur'}" /></th>
						<th><input st-search="storageType" placeholder="Storage" class="input-sm form-control" type="search" style="text-align: center;" ng-model-options="{updateOn:'blur'}" /></th>
						<th><input st-search="partitions" class="input-sm form-control" type="search" style="text-align: center;" ng-model-options="{updateOn:'blur'}" /></th>
						<th><input st-search="consumerRetryPolicy" placeholder="Policy" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
						<th><input st-search="ackTimeoutSeconds" placeholder="ACK Timeout" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
						<th><input st-search="endpointType" placeholder="Endpoint" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
						<th></th>
					</tr>
				</thead>
				<tbody ng-show="!is_loading">
					<tr ng-repeat="row in topic_rows">
						<td><a href="${model.webapp}/console/topic?op=detail&topic={{row.name}}" ng-bind="row.name" /></td>
						<td align="center"><span ng-bind="row.codecType"></span></td>
						<td align="center"><span ng-bind="row.storageType"></td>
						<td align="center"><span ng-bind="row.partitions"></td>
						<td><span ng-bind="row.consumerRetryPolicy"></td>
						<td><span ng-bind="row.ackTimeoutSeconds"></td>
						<td><span ng-bind="row.endpointType"></td>
						<td>
							<button type="button" ng-click="del_topic(row.name)" class="btn btn-xs btn-danger" style="text-align: center;">删除</button>
						</td>
					</tr>
				</tbody>
				<tbody ng-show="is_loading">
					<tr>
						<td colspan="9" class="text-center">Loading ...</td>
					</tr>
				</tbody>
			</table>
		</div>
		<div class="modal fade" id="add-topic-modal" tabindex="-1" role="dialog" aria-labelledby="add-topic-label" aria-hidden="true">
			<div class="modal-dialog" style="width: 50%">
				<div class="modal-content">
					<div class="modal-header">
						<button type="button" class="close" data-dismiss="modal" aria-label="Close">
							<span aria-hidden="true">&times;</span>
						</button>
						<h4 class="modal-title" id="add-topic-label">新增 Topic</h4>
					</div>
					<div class="modal-body">
						<form class="form-horizontal">
							<div class="form-group">
								<label for="inputTopicName" class="col-sm-4 control-label">名称</label>
								<div class="col-sm-6">
									<input class="form-control" id="inputTopicName" placeholder="Topic" ng-model="new_topic.name">
								</div>
							</div>
							<div class="form-group">
								<label for="inputCodec" class="col-sm-4 control-label">编码类型</label>
								<div class="col-sm-4">
									<select class="form-control" id="inputCodec" ng-model="new_topic.codecType" ng-options="codec for codec in codec_types">
									</select>
								</div>
							</div>
							<div class="form-group">
								<label for="inputStorageType" class="col-sm-4 control-label">存储类型</label>
								<div class="col-sm-4">
									<select class="form-control" id="inputStorageType" ng-model="new_topic.storageType" ng-options="storage for storage in storage_types" ng-change="storage_type_changed()">
									</select>
								</div>
							</div>
							<div class="form-group">
								<label for="inputEndpointType" class="col-sm-4 control-label">Endpoint 类型</label>
								<div class="col-sm-4">
									<select class="form-control" id="inputEndpointType" ng-model="new_topic.endpointType" ng-options="endpoint for endpoint in endpoint_types" ng-change="endpoint_type_changed()">
									</select>
								</div>
							</div>
							<div class="form-group">
								<label for="inputAckTimeout" class="col-sm-4 control-label">ACK 超时(秒)</label>
								<div class="col-sm-4">
									<input class="form-control" id="inputAckTimeout" placeholder="ACK Timeout" ng-model="new_topic.ackTimeoutSeconds">
								</div>
							</div>
							<div class="form-group">
								<label for="inputConsumeRetryPolicy" class="col-sm-4 control-label">消费重试策略</label>
								<div class="col-sm-6">
									<input class="form-control" id="inputConsumeRetryPolicy" placeholder="Consume Retry Policy" ng-model="new_topic.consumeRetryPolicy">
								</div>
							</div>
						</form>
					</div>
					<div class="modal-header">
						<button type="button" class="btn btn-xs btn-success" style="float: right; margin-left: 10px" ng-click="add_new_partition()">新增</button>
						<button type="button" class="btn btn-xs btn-danger" style="float: right; margin-left: 10px" ng-click="del_one_partition()">减少</button>
						<h4 class="modal-title" id="topic-partitions-label">Partitions</h4>
					</div>
					<div class="modal-body" style="max-height: 500px;">
						<table class="table table-condensed table-responsive">
							<thead>
								<tr>
									<th style="border: none; width: 5%">#</th>
									<th style="border: none;">Read DS</th>
									<th style="border: none;">Write DS</th>
									<th style="border: none;">Endpoint</th>
								</tr>
							</thead>
							<tbody>
								<tr ng-repeat="partition in new_topic.partitions">
									<td style="border: none; width: 5%;"><label style="line-height: 2.2" ng-bind="$index + 1"></label></td>
									<td style="border: none;"><select name="rds" class="form-control" id="inputReadDatasource" ng-model="partition.readDatasource" ng-options="ds for ds in datasource_names"></select></td>
									<td style="border: none;"><select name="wds" class="form-control" id="inputWriteDatasource" ng-model="partition.writeDatasource" ng-options="ds for ds in datasource_names"></select></td>
									<td style="border: none;"><select name="edp" class="form-control" id="inputEndpoint" ng-model="partition.endpoint" ng-options="ep for ep in endpoint_names"></select></td>
								</tr>
							</tbody>
						</table>
					</div>
					<div class="modal-footer">
						<button type="button" class="btn btn-danger" data-dismiss="modal">关闭</button>
						<button type="button" class="btn btn-success" data-dismiss="modal" ng-click="add_topic(new_topic)">保存</button>
					</div>
				</div>
			</div>
		</div>
	</div>

	<script type="text/javascript" src="${model.webapp}/js/angular.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/angular-resource.min.js"></script>

	<script type="text/javascript" src="${model.webapp}/js/topic.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/smart-table.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/bootbox.min.js"></script>
</a:layout>