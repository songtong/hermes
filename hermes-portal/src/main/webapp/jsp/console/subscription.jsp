<%@ page contentType="text/html; charset=utf-8" isELIgnored="false" trimDirectiveWhitespaces="true"%>
<%@ taglib prefix="a" uri="/WEB-INF/app.tld"%>
<jsp:useBean id="ctx" type="com.ctrip.hermes.portal.console.subscription.Context" scope="request" />
<jsp:useBean id="payload" type="com.ctrip.hermes.portal.console.subscription.Payload" scope="request" />
<jsp:useBean id="model" type="com.ctrip.hermes.portal.console.subscription.Model" scope="request" />

<a:layout>
	<link href="${model.webapp}/css/xeditable.css" type="text/css" rel="stylesheet">
	<div ng-app="hermes-subscription" ng-controller="subscription-controller">
		<div class="panel panel-info">
			<div class="panel-heading">Hermes Subscriber 列表</div>
			<table class="table table-condensed table-responsive table-bordered" st-table="display_subscribers" st-safe-src="subscribers">
				<thead>
					<tr>
						<th style="width: 5%">#</th>
						<th st-sort="name">Name</th>
						<th st-sort="topic">Topic</th>
						<th st-sort="group">Consumer</th>
						<th st-sort="endpoints">Endpoints</th>
						<th st-sort="status" width="20px">Status</th>
						<th width="100px"><button class="btn btn-success btn-xs" ng-click="add_row()"><span class="glyphicon glyphicon-plus"></span> 新增</button></th>
					</tr>
				</thead>
				<tbody>
					<tr ng-repeat="sb in display_subscribers">
						<td style="width: 5%;"><label ng-bind="$index + 1"></label></td>
						<td><span editable-text="sb.name" ng-bind="sb.name || 'Not Set'" e-name="name" e-form="rowform" onbeforesave="checkName($data, sb.id)"  e-required></span></td>
						<td><span editable-text="sb.topic" ng-bind="sb.topic || 'Not Set'" e-name="topic" e-form="rowform" e-typeahead="topic for topic in topic_names | filter:$viewValue | limitTo:8"></span></td>
						<td><span editable-text="sb.group" ng-bind="sb.group || 'Not Set'" e-name="group" e-form="rowform" e-typeahead="consumer for consumer in consumer_names | filter:$viewValue | limitTo:8"></span></td>
						<td><span editable-text="sb.endpoints" ng-bind="sb.endpoints || 'Not Set'" e-name="endpoints" e-form="rowform"></span></td>
						<td style="text-align: center;"><span ng-if="sb.status=='STOPPED'" tooltip="{{sb.status}}" class="status-danger"></span> <span ng-if="sb.status=='RUNNING'" tooltip="{{sb.status}}"
								class="status-ok"></span></td>
						<td width="180px">
							<form shown="inserted == sb"  onaftersave="add_subscriber($data,sb.id,sb.status)" name="rowform" ng-show="rowform.$visible" class="form-buttons form-inline" editable-form>
								<button type="submit" ng-disabled="rowform.$waiting" class="btn btn-primary btn-xs"><span class="glyphicon glyphicon-floppy-save"></span> 保存</button>
								<button type="button" ng-disabled="rowform.$waiting" ng-click="rowform.$cancel()" class="btn btn-danger btn-xs"><span class="glyphicon glyphicon-log-out"></span> 取消</button>
							</form>
							<div class="buttons" ng-show="!rowform.$visible">
								<button class="btn btn-success btn-xs" ng-if="sb.status=='STOPPED'" ng-click="start_subscription(sb)"><span class="glyphicon glyphicon-play"></span> 启用</button>
								<button class="btn btn-danger btn-xs" ng-if="sb.status=='RUNNING'" ng-click="stop_subscription(sb)"><span class="glyphicon glyphicon-pause"></span> 停用</button>
								<button class="btn btn-warning btn-xs" ng-click="rowform.$show()"><span class="glyphicon glyphicon-edit"></span> 修改</button>
								<button class="btn btn-danger btn-xs" ng-click="del_row(sb)"><span class="glyphicon glyphicon-remove"></span> 删除</button>
							</div>
						</td>
					</tr>
				</tbody>
			</table>
		</div>
	</div>

	<script type="text/javascript" src="${model.webapp}/js/angular/xeditable.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/angular/smart-table.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/subscription/subscription.js"></script>
</a:layout>