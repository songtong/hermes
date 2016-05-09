<%@ page contentType="text/html; charset=utf-8" isELIgnored="false" trimDirectiveWhitespaces="true"%>
<%@ taglib prefix="a" uri="/WEB-INF/app.tld"%>

<jsp:useBean id="ctx" type="com.ctrip.hermes.portal.console.consumer.Context" scope="request" />
<jsp:useBean id="payload" type="com.ctrip.hermes.portal.console.consumer.Payload" scope="request" />
<jsp:useBean id="model" type="com.ctrip.hermes.portal.console.consumer.Model" scope="request" />

<a:layout>
	<link href="${model.webapp}/css/xeditable.css" type="text/css" rel="stylesheet">
	<link href="${model.webapp}/css/font-awesome.min.css" type="text/css" rel="stylesheet">
	<div ng-app="hermes-consumer" ng-controller="consumer-controller">
		<div class="panel panel-info">
			<div class="panel-heading">Hermes 消费者列表</div>
			<table class="table table-hover" ng-if="consumers" st-safe-src="consumers" st-table="filteredConsumers">
				<thead>
					<tr>
						<th st-sort="name">消费集群</th>
						<th st-sort="appIds">应用</th>
						<th st-sort="topicName">Topic名称</th>
						<th st-sort="orderedConsume">有序</th>
						<th st-sort="retryPolicy">消费重试策略</th>
						<th st-sort="ackTimeoutSeconds">ACK超时</th>
						<th st-sort="owner1">负责人1</th>
						<th style="text-align: left;"><button type="button" data-toggle="modal" data-target="#add-consumer-modal" class="btn btn-xs btn-success" style="text-align: center;">新增</button></th>
					</tr>
					<tr>
						<th><input st-search="name" placeholder="Group" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
						<th><input st-search="appIds" placeholder="App" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
						<th><input st-search="topicName" placeholder="Topic" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
						<th><input st-search="orderedConsume" placeholder="Group" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
						<th><input st-search="retryPolicy" placeholder="Retry" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
						<th><input st-search="ackTimeoutSeconds" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
						<th></th>
					</tr>
				</thead>
				<tbody>
					<tr ng-repeat="consumer in filteredConsumers">
						<td><span e-form="rowform" ng-bind="consumer.name" e-name="name"></td>
						<td><span e-form="rowform" editable-text="consumer.appIds" ng-bind="consumer.appIds" e-name="appIds"></td>
						<td><span e-form="rowform" ng-bind="consumer.topicName" e-name="topicName"></td>
						<td><span e-form="rowform" editable-select="consumer.orderedConsume" ng-bind="consumer.orderedConsume" e-name="orderedConsume" e-ng-options="order for order in order_opts"></td>
						<td><span e-form="rowform" editable-text="consumer.retryPolicy" ng-bind="consumer.retryPolicy" e-name="retryPolicy"></td>
						<td><span e-form="rowform" editable-text="consumer.ackTimeoutSeconds" ng-bind="consumer.ackTimeoutSeconds" e-name="ackTimeoutSeconds"></td>
						<td><span e-form="rowform" editable-text="consumer.owner1" ng-bind="consumer.owner1" e-name="owner1"></td>
						<td style="white-space: nowrap">
							<form editable-form name="rowform" onbeforesave="update_consumer($data,consumer.topicName,consumer.name)" ng-show="rowform.$visible">
								<button type="submit" ng-disabled="rowform.$waiting" class="btn btn-xs btn-primary">保存</button>
								<button type="button" ng-disabled="rowform.$waiting" ng-click="rowform.$cancel()" class="btn btn-xs btn-default">取消</button>
							</form>
							<div class="buttons" ng-show="!rowform.$visible">
								<button type="button" ng-click="rowform.$show()" class="btn btn-xs btn-warning" style="text-align: center;">修改</button>
								<button type="button" ng-click="confirmDeletion(consumer.topicName, consumer.name)" class="btn btn-xs btn-danger" style="text-align: center;">删除</button>
								<button type="button" data-toggle="modal" data-target="#resetOffsetModal" class="btn btn-xs btn-primary" style="text-align: center;" ng-click="setCurrentConsumer(consumer)">Offset</button>
							</div>
						</td>
					</tr>
				</tbody>
				<tfoot>
					<tr>
						<td class="text-center" colspan="7" pagination-x page-size="10">
						</td>
					</tr>
				</tfoot>
			</table>
		</div>
		<div class="modal fade" id="resetOffsetModal" tabindex="-1" role="dialog" aria-labelledby="resetOffsetModal" aria-hidden="true">
		<div class="modal-dialog">
				<div class="modal-content">
					<div class="modal-header">
						<button type="button" class="close" data-dismiss="modal" aria-label="Close">
							<span aria-hidden="true">&times;</span>
						</button>
						<h4 class="modal-title" id="add-consumer-label">Offset重置</h4>
					</div>
					<div class="modal-body">
						<form class="form-horizontal" name="offsetForm">
							<div class="form-group">
								<label for="consumerName" class="col-sm-3 control-label">Consumer 名称</label>
								<div class="col-sm-8">
									<input type="text" class="form-control" id="consumerName" placeholder="Consumer Name" ng-model="currentConsumer.name" readonly>
								</div>
							</div>
							<div class="form-group">
								<label for="resetOption" class="col-sm-3 control-label">重置</label>
								<div class="col-sm-8">
									<div id="resetOptions" class="btn-group" data-toggle="buttons">
									  <label class="btn btn-primary active" ng-click="expandCollapse($event)">
									    <input type="radio" name="offsetOption" id="option1" value="latest" autocomplete="off" checked>最新
									  </label>
									  <label class="btn btn-primary" ng-click="expandCollapse($event)">
									    <input type="radio" name="offsetOption" id="option2" value="earliest" autocomplete="off">最早
									  </label>
									  <label class="btn btn-primary" ng-click="expandCollapse($event)">
									    <input type="radio" name="offsetOption" id="option3" value="timepoint" autocomplete="off" >时间点
									  </label>
									</div>
								</div>
							</div>
							<div class="form-group collapse" id="timepoint">
								<label class="col-sm-3 control-label">&nbsp;</label>
								<div class="col-sm-8">
									<input type="datetime-local" ng-model="currentTimestamp"
      										placeholder="yyyy-MM-ddTHH:mm:ss" min="2015-01-01T00:00:00" ng-max="maxTimestamp" />
								</div>
							</div>
							<div class="form-group">
								<div class="col-sm-11">
									<button class="btn btn-primary pull-right" ng-click="offsetForm.$valid && submitReset()">提交</button>
								</div>
							</div>
						</form>
					</div>
				</div>
		</div>
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
								<label for="inputName" class="col-sm-3 control-label">消费集群名称</label>
								<div class="col-sm-8">
									<input class="form-control" id="inputName" placeholder="Consumer Group Name" ng-model="newConsumer.name">
								</div>
							</div>
							<div class="form-group">
								<label for="inputOrderedConsume" class="col-sm-3 control-label">保证按序消费</label>
								<div class="col-sm-3">
									<select name="ordered-consume" class="form-control" id="inputOrderedConsume" ng-model="newConsumer.orderedConsume" ng-options="order for order in order_opts">
									</select>
								</div>
							</div>
							<div class="form-group">
								<label for="inputAppIds" class="col-sm-3 control-label">应用名称</label>
								<div class="col-sm-8">
									<input class="form-control" id="inputAppIds" placeholder="App Name" ng-model="newConsumer.appIds">
								</div>
							</div>
							<div class="form-group">
								<label for="inputRetryPolicy" class="col-sm-3 control-label">消费重试策略</label>
								<div class="col-sm-8">
									<input class="form-control" id="inputRetryPolicy" placeholder="Consume Retry Policy" ng-model="newConsumer.retryPolicy">
								</div>
							</div>
							<div class="form-group">
								<label for="inputAckTimeoutSeconds" class="col-sm-3 control-label">ACK 超时</label>
								<div class="col-sm-3">
									<input class="form-control" id="inputAckTimeoutSeconds" placeholder="ACK Timeout in Seconds" ng-model="newConsumer.ackTimeoutSeconds">
								</div>
							</div>
							<div class="form-group">
								<label for="inputOwner1" class="col-sm-3 control-label">负责人1</label>
								<div class="col-sm-3">
									<input class="form-control" id="inputOwner1" ng-model="newConsumer.owner1">
								</div>
							</div>
							<div class="form-group">
								<label for="inputPhone1" class="col-sm-3 control-label">电话1</label>
								<div class="col-sm-3">
									<input class="form-control" id="inputPhone1" ng-model="newConsumer.phone1">
								</div>
							</div>
							<div class="form-group">
								<label for="inputOwner2" class="col-sm-3 control-label">负责人2</label>
								<div class="col-sm-3">
									<input class="form-control" id="inputOwner2" ng-model="newConsumer.owner2">
								</div>
							</div>
							<div class="form-group">
								<label for="inputPhone2" class="col-sm-3 control-label">电话2</label>
								<div class="col-sm-3">
									<input class="form-control" id="inputPhone2" ng-model="newConsumer.phone2">
								</div>
							</div>
						</form>
					</div>
					<div class="modal-footer">
						<button type="button" class="btn btn-sm btn-danger" data-dismiss="modal">关闭</button>
						<button type="button" class="btn btn-sm btn-success" data-dismiss="modal" ng-click="addConsumer(newConsumer)">保存</button>
					</div>
				</div>
			</div>
		</div>
		<loading-x></loading-x>
		<confirm-dialog-x id="deletionConfirm" title="删除" content="您确定要删除Consumer[{{consumerName}}]" action="deleteConsumer"></confirm-dialog-x>
	</div>

	<script type="text/javascript" src="${model.webapp}/js/angular/xeditable.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/angular/smart-table.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/lib/date.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/consumer/consumer.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/lib/global.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/lib/utils.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/lib/components.js"></script>
</a:layout>