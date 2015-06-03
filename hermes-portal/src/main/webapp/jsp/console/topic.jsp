<%@ page contentType="text/html; charset=utf-8" isELIgnored="false" trimDirectiveWhitespaces="true"%>
<%@ taglib prefix="a" uri="/WEB-INF/app.tld"%>


<jsp:useBean id="ctx" type="com.ctrip.hermes.portal.console.topic.Context" scope="request" />
<jsp:useBean id="payload" type="com.ctrip.hermes.portal.console.topic.Payload" scope="request" />
<jsp:useBean id="model" type="com.ctrip.hermes.portal.console.topic.Model" scope="request" />

<a:layout>
	<script type="text/javascript" src="${model.webapp}/js/angular.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/angular-resource.min.js"></script>

	<script type="text/javascript" src="${model.webapp}/js/topic.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/smart-table.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/bootbox.min.js"></script>
	<div class="op-alert alert alert-info" role="alert" style="display: none;">
		<span>The examples populate this alert with dummy content</span>
	</div>
	<div ng-app="hermes-topic" ng-controller="topic-controller">
		<div class="panel panel-info">
			<div class="panel-heading">Hermes消息主题列表</div>
			<table class="table table-hover" st-pipe="get_topics" st-table="topic_table">
				<thead>
					<tr>
						<th st-sort="name">Topic名称</th>
						<th st-sort="codecType" width="100px" style="text-align: center;">编码</th>
						<th st-sort="storageType" width="100px" style="text-align: center;">存储</th>
						<th st-sort="schemaName">Schema</th>
						<th st-sort="partitions" width="60px" style="text-align: center;">分区</th>
						<th st-sort="consumerRetryPolicy">消费重试策略</th>
						<th st-sort="ackTimeoutSeconds">ACK超时</th>
						<th st-sort="endpointType">Endpoint</th>
						<th style="text-align: left;"><button type="button" data-toggle="modal" data-target="#add-topic-modal" class="btn btn-xs btn-success" style="text-align: center;">新增</button></th>
					</tr>
					<tr>
						<th><input st-search="name" placeholder="Topic" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
						<th><input st-search="codecType" placeholder="Codec" class="input-sm form-control" type="search" style="text-align: center;" ng-model-options="{updateOn:'blur'}" /></th>
						<th><input st-search="storageType" placeholder="Storage" class="input-sm form-control" type="search" style="text-align: center;" ng-model-options="{updateOn:'blur'}" /></th>
						<th><input st-search="schemaName" placeholder="Schema" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
						<th><input st-search="partitions" class="input-sm form-control" type="search" style="text-align: center;" ng-model-options="{updateOn:'blur'}" /></th>
						<th><input st-search="consumerRetryPolicy" placeholder="Policy" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
						<th><input st-search="ackTimeoutSeconds" placeholder="ACK Timeout" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
						<th><input st-search="endpointType" placeholder="Endpoint" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
						<th></th>
					</tr>
				</thead>
				<tbody ng-show="!is_loading">
					<tr ng-repeat="row in topic_rows">
						<td>{{row.name}}</td>
						<td align="center">{{row.codecType}}</td>
						<td align="center">{{row.storageType}}</td>
						<td>{{row.schemaName}}</td>
						<td align="center">{{row.partitions}}</td>
						<td>{{row.consumerRetryPolicy}}</td>
						<td>{{row.ackTimeoutSeconds}}</td>
						<td>{{row.endpointType}}</td>
						<td>
							<button type="button" ng-click="updateTopic()" class="btn btn-xs btn-warning" style="text-align: center;">修改</button>
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
			<div class="modal-dialog">
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
								<label for="inputTopicName" class="col-sm-3 control-label">名称</label>
								<div class="col-sm-8">
									<input class="form-control" id="inputTopicName" placeholder="Topic" ng-model="new_topic.name">
								</div>
							</div>
							<div class="form-group">
								<label for="inputCodec" class="col-sm-3 control-label">编码类型</label>
								<div class="col-sm-4">
									<select name="codec-type" class="form-control" id="inputCodec" ng-model="new_topic.codecType">
										<option>json</option>
										<option>avro</option>
									</select>
								</div>
							</div>
							<div class="form-group">
								<label for="inputStorageType" class="col-sm-3 control-label">存储类型</label>
								<div class="col-sm-4">
									<select name="storage-type" class="form-control" id="inputStorageType" ng-model="new_topic.storageType">
										<option>mysql</option>
										<option>kafka</option>
										<option>memory</option>
									</select>
								</div>
							</div>
							<div class="form-group">
								<label for="inputEndpointType" class="col-sm-3 control-label">Endpoint 类型</label>
								<div class="col-sm-4">
									<select name="endpoint-type" class="form-control" id="inputEndpointType" ng-model="new_topic.endpointType">
										<option>broker</option>
										<option>kafka</option>
									</select>
								</div>
							</div>
							<div class="form-group">
								<label for="inputAckTimeout" class="col-sm-3 control-label">ACK 超时</label>
								<div class="col-sm-4">
									<input class="form-control" id="inputAckTimeout" placeholder="ACK Timeout" ng-model="new_topic.ackTimeoutSeconds">
								</div>
							</div>
							<div class="form-group">
								<label for="inputConsumeRetryPolicy" class="col-sm-3 control-label">消费重试策略</label>
								<div class="col-sm-8">
									<input class="form-control" id="inputConsumeRetryPolicy" placeholder="Consume Retry Policy" ng-model="new_topic.consumeRetryPolicy">
								</div>
							</div>
							<div class="form-group">
								<label for="inputReadDatasource" class="col-sm-3 control-label">Partition</label>
								<div class="col-sm-6">
									<div class="input-group">
										<input class="form-control" id="inputReadDatasource" placeholder="Read Datasource" ng-model="new_topic.partition.readDatasource">
										<div class="input-group-addon">读源</div>
									</div>
								</div>
							</div>
							<div class="form-group">
								<label for="inputWriteDatasource" class="col-sm-3 control-label"></label>
								<div class="col-sm-6">
									<div class="input-group">
										<input class="form-control" id="inputWriteDatasource" placeholder="Write Datasource" ng-model="new_topic.partition.writeDatasource">
										<div class="input-group-addon">写源</div>
									</div>
								</div>
							</div>
							<div class="form-group">
								<label for="inputEndpoint" class="col-sm-3 control-label"></label>
								<div class="col-sm-6">
									<div class="input-group">
										<input class="form-control" id="inputEndpoint" placeholder="Endpoint" ng-model="new_topic.partition.endpoint">
										<div class="input-group-addon">Endpoint</div>
									</div>
								</div>
							</div>
						</form>
					</div>
					<div class="modal-footer">
						<button type="button" class="btn btn-sm btn-danger" data-dismiss="modal">关闭</button>
						<button type="button" class="btn btn-sm btn-success" data-dismiss="modal" ng-click="add_topic(new_topic)">保存</button>
					</div>
				</div>
			</div>
		</div>
	</div>
</a:layout>