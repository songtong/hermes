<%@ page contentType="text/html; charset=utf-8" isELIgnored="false" trimDirectiveWhitespaces="true"%>
<%@ taglib prefix="a" uri="/WEB-INF/app.tld"%>


<jsp:useBean id="ctx" type="com.ctrip.hermes.portal.console.topic.Context" scope="request" />
<jsp:useBean id="payload" type="com.ctrip.hermes.portal.console.topic.Payload" scope="request" />
<jsp:useBean id="model" type="com.ctrip.hermes.portal.console.topic.Model" scope="request" />

<a:layout>
	<link href="${model.webapp}/css/topic.css" type="text/css" rel="stylesheet">
	<script type="text/javascript" src="${model.webapp}/js/angular.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/angular-resource.min.js"></script>

	<script type="text/javascript" src="${model.webapp}/js/topic.service.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/topic.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/smart-table.min.js"></script>
	<div ng-app="hermes-topic" ng-controller="topic-controller">
		<div class="panel panel-info">
			<div class="panel-heading">Hermes Topics</div>
			<table class="table table-hover" st-pipe="get_topics" st-table="topic_rows">
				<thead>
					<tr>
						<th st-sort="name">Topic 名称</th>
						<th st-sort="codecType">编码</th>
						<th st-sort="storageType">存储</th>
						<th st-sort="partitions">Partitions</th>
						<th st-sort="cpolicy">消费策略</th>
						<th st-sort="endpointType">Endpoint</th>
						<th style="text-align: left;"><button type="button" data-toggle="modal" data-target="#add-topic-modal" class="btn btn-xs btn-success"
								style="text-align: center;">新增</button></th>
					</tr>
					<tr>
						<th><input st-search="name" placeholder="查找 Topic" class="input-sm form-control" type="search" /></th>
						<th><input st-search="codecType" placeholder="查找编码" class="input-sm form-control" type="search" /></th>
						<th><input st-search="storageType" placeholder="查找存储" class="input-sm form-control" type="search" /></th>
						<th><input st-search="partitions" placeholder="查找分区数量" class="input-sm form-control" type="search" /></th>
						<th><input st-search="cpolicy" placeholder="查找消费策略" class="input-sm form-control" type="search" /></th>
						<th><input st-search="endpointType" placeholder="查找 Endpoint" class="input-sm form-control" type="search" /></th>
						<th></th>
					</tr>
				</thead>
				<tbody ng-show="!is_loading">
					<tr ng-repeat="row in topic_rows">
						<td>{{row.name}}</td>
						<td>{{row.codecType}}</td>
						<td>{{row.storageType}}</td>
						<td>{{row.partitions}}</td>
						<td>{{row.cpolicy}}</td>
						<td>{{row.endpointType}}</td>
						<td>
							<button type="button" ng-click="updateTopic()" class="btn btn-xs btn-warning" style="text-align: center;">修改</button>
							<button type="button" ng-click="removeTopic()" class="btn btn-xs btn-danger" style="text-align: center;">删除</button>
						</td>
					</tr>
				</tbody>
				<tbody ng-show="is_loading">
					<tr>
						<td colspan="7" class="text-center">Loading ...</td>
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
									</select>
								</div>
							</div>
							<div class="form-group">
								<label for="inputReadDatasource" class="col-sm-3 control-label">Partition</label>
								<div class="col-sm-8">
									<div class="input-group">
										<input class="form-control" id="inputReadDatasource" placeholder="Read Datasource" ng-model="new_topic.partition_readds">
										<div class="input-group-addon">读源</div>
									</div>
								</div>
							</div>
							<div class="form-group">
								<label for="inputWriteDatasource" class="col-sm-3 control-label"></label>
								<div class="col-sm-8">
									<div class="input-group">
										<input class="form-control" id="inputWriteDatasource" placeholder="Write Datasource" ng-model="new_topic.partition_writeds">
										<div class="input-group-addon">写源</div>
									</div>
								</div>
							</div>
							<div class="form-group">
								<label for="inputEndpoint" class="col-sm-3 control-label"></label>
								<div class="col-sm-8">
									<div class="input-group">
										<input class="form-control" id="inputEndpoint" placeholder="Endpoint" ng-model="new_topic.partition_endpoint">
										<div class="input-group-addon">Endpoint</div>
									</div>
								</div>
							</div>
							<div class="form-group">
								<label for="inputConsumeRetryPolicy" class="col-sm-3 control-label">消费策略</label>
								<div class="col-sm-8">
									<input class="form-control" id="inputConsumeRetryPolicy" placeholder="Consume Retry Policy" ng-model="new_topic.consumeRetryPolicy">
								</div>
							</div>
							<div class="form-group">
								<label for="inputEndpoint" class="col-sm-3 control-label">Endpoint</label>
								<div class="col-sm-8">
									<input class="form-control" id="inputEndpoint" placeholder="Default Endpoint" ng-model="new_topic.endpoint">
								</div>
							</div>
						</form>
					</div>
					<div class="modal-footer">
						<button type="button" class="btn btn-sm btn-danger" data-dismiss="modal">关闭</button>
						<button type="button" class="btn btn-sm btn-success" ng-click="add_topic(new_topic)">保存</button>
					</div>
				</div>
			</div>
		</div>
	</div>
</a:layout>