<%@ page contentType="text/html; charset=utf-8" isELIgnored="false" trimDirectiveWhitespaces="true"%>
<%@ taglib prefix="a" uri="/WEB-INF/app.tld"%>

<jsp:useBean id="ctx" type="com.ctrip.hermes.portal.console.consumer.Context" scope="request" />
<jsp:useBean id="payload" type="com.ctrip.hermes.portal.console.consumer.Payload" scope="request" />
<jsp:useBean id="model" type="com.ctrip.hermes.portal.console.consumer.Model" scope="request" />

<a:layout>
	<link href="${model.webapp}/css/xeditable.css" type="text/css" rel="stylesheet">
	<div ng-app="hermes-consumer" ng-controller="consumer-controller">
		<div class="panel panel-info">
			<div class="panel-heading">Hermes 消费者列表</div>
			<table class="table table-hover" st-pipe="get_consumers" st-table="consumer_table">
				<thead>
					<tr>
						<th st-sort="groupName">消费集群</th>
						<th st-sort="appId">应用</th>
						<th st-sort="topicName">Topic名称</th>
						<th st-sort="orderedConsume">有序</th>
						<th st-sort="retryPolicy">消费重试策略</th>
						<th st-sort="ackTimeoutSeconds">ACK超时</th>
						<th style="text-align: left;"><button type="button" data-toggle="modal" data-target="#add-consumer-modal" class="btn btn-xs btn-success" style="text-align: center;">新增</button></th>
					</tr>
					<tr>
						<th><input st-search="groupName" placeholder="Group" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
						<th><input st-search="appId" placeholder="App" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
						<th><input st-search="topicName" placeholder="Topic" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
						<th><input st-search="orderedConsume" placeholder="Group" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
						<th><input st-search="retryPolicy" placeholder="Retry" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
						<th><input st-search="ackTimeoutSeconds" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
						<th></th>
					</tr>
				</thead>
				<tbody ng-show="!is_loading">
					<tr ng-repeat="row in consumer_rows">
						<td><span e-form="rowform" ng-bind="row.groupName" e-name="groupName"></td>
						<td><span e-form="rowform" editable-text="row.appId" ng-bind="row.appId" e-name="appId"></td>
						<td><span e-form="rowform" ng-bind="row.topicName" e-name="topicName"></td>
						<td><span e-form="rowform" editable-select="row.orderedConsume" ng-bind="row.orderedConsume" e-name="orderedConsume" e-ng-options="order for order in order_opts"></td>
						<td><span e-form="rowform" editable-text="row.retryPolicy" ng-bind="row.retryPolicy" e-name="retryPolicy"></td>
						<td><span e-form="rowform" editable-text="row.ackTimeoutSeconds" ng-bind="row.ackTimeoutSeconds" e-name="ackTimeoutSeconds"></td>
						<td style="white-space: nowrap">
							<form editable-form name="rowform" onbeforesave="update_consumer($data,row.topicName,row.groupName)" ng-show="rowform.$visible">
								<button type="submit" ng-disabled="rowform.$waiting" class="btn btn-xs btn-primary">保存</button>
								<button type="button" ng-disabled="rowform.$waiting" ng-click="rowform.$cancel()" class="btn btn-xs btn-default">取消</button>
							</form>
							<div class="buttons" ng-show="!rowform.$visible">
								<button type="button" ng-click="rowform.$show()" class="btn btn-xs btn-warning" style="text-align: center;">修改</button>
								<button type="button" ng-click="del_consumer(row.topicName, row.groupName)" class="btn btn-xs btn-danger" style="text-align: center;">删除</button>
							</div>
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
		<div class="modal fade" id="add-consumer-modal" tabindex="-1" role="dialog" aria-labelledby="add-consumer-label" aria-hidden="true">
			<div class="modal-dialog">
				<div class="modal-content">
					<div class="modal-header">
						<button type="button" class="close" data-dismiss="modal" aria-label="Close">
							<span aria-hidden="true">&times;</span>
						</button>
						<h4 class="modal-title" id="add-consumer-label">新增 Consumer</h4>
					</div>
					<div class="modal-body">
						<form class="form-horizontal">
							<div class="form-group">
								<label for="inputTopicName" class="col-sm-3 control-label">Topic名称</label>
								<div class="col-sm-8">
									<input type="text" class="form-control" id="inputTopicName" placeholder="Topic Name" ng-model="newTopicNames">
								</div>
							</div>
							<div class="form-group">
								<label for="inputGroupName" class="col-sm-3 control-label">消费集群名称</label>
								<div class="col-sm-8">
									<input class="form-control" id="inputGroupName" placeholder="Consumer Group Name" ng-model="new_consumer.groupName">
								</div>
							</div>
							<div class="form-group">
								<label for="inputOrderedConsume" class="col-sm-3 control-label">保证按序消费</label>
								<div class="col-sm-3">
									<select name="ordered-consume" class="form-control" id="inputOrderedConsume" ng-model="new_consumer.orderedConsume" ng-options="order for order in order_opts">
									</select>
								</div>
							</div>
							<div class="form-group">
								<label for="inputAppId" class="col-sm-3 control-label">应用名称</label>
								<div class="col-sm-8">
									<input class="form-control" id="inputAppId" placeholder="App Name" ng-model="new_consumer.appId">
								</div>
							</div>
							<div class="form-group">
								<label for="inputRetryPolicy" class="col-sm-3 control-label">消费重试策略</label>
								<div class="col-sm-8">
									<input class="form-control" id="inputRetryPolicy" placeholder="Consume Retry Policy" ng-model="new_consumer.retryPolicy">
								</div>
							</div>
							<div class="form-group">
								<label for="inputAckTimeoutSeconds" class="col-sm-3 control-label">ACK 超时</label>
								<div class="col-sm-3">
									<input class="form-control" id="inputAckTimeoutSeconds" placeholder="ACK Timeout in Seconds" ng-model="new_consumer.ackTimeoutSeconds">
								</div>
							</div>
						</form>
					</div>
					<div class="modal-footer">
						<button type="button" class="btn btn-sm btn-danger" data-dismiss="modal">关闭</button>
						<button type="button" class="btn btn-sm btn-success" data-dismiss="modal" ng-click="add_consumer(new_consumer)">保存</button>
					</div>
				</div>
			</div>
		</div>
	</div>

	<script type="text/javascript" src="${model.webapp}/js/angular/xeditable.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/angular/smart-table.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/consumer/consumer.js"></script>
</a:layout>