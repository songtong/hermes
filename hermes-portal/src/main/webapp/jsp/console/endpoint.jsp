<%@ page contentType="text/html; charset=utf-8" isELIgnored="false" trimDirectiveWhitespaces="true"%>
<%@ taglib prefix="a" uri="/WEB-INF/app.tld"%>
<jsp:useBean id="ctx" type="com.ctrip.hermes.portal.console.endpoint.Context" scope="request" />
<jsp:useBean id="payload" type="com.ctrip.hermes.portal.console.endpoint.Payload" scope="request" />
<jsp:useBean id="model" type="com.ctrip.hermes.portal.console.endpoint.Model" scope="request" />

<a:layout>
	<div class="op-alert alert alert-info" role="alert" style="display: none;">
		<span>The examples populate this alert with dummy content</span>
	</div>
	<div ng-app="hermes-endpoint" ng-controller="endpoint-controller">
		<div class="panel panel-info">
			<div class="panel-heading">Hermes Endpoint 列表</div>
			<table class="table table-hover" st-pipe="get_endpoints" st-table="endpoint_table">
				<thead>
					<tr>
						<th st-sort="id">名称</th>
						<th st-sort="type">类型</th>
						<th st-sort="host">Host</th>
						<th st-sort="port">Port</th>
						<th style="text-align: left;"><button type="button" data-toggle="modal" data-target="#add-endpoint-modal" class="btn btn-xs btn-success" style="text-align: center;">新增</button></th>
					</tr>
					<tr>
						<th><input st-search="id" placeholder="Name" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
						<th><input st-search="type" placeholder="Type" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
						<th><input st-search="host" placeholder="Host" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
						<th><input st-search="port" placeholder="Port" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
						<th></th>
					</tr>
				</thead>
				<tbody ng-show="!is_loading">
					<tr ng-repeat="row in endpoint_rows">
						<td><span ng-bind="row.id"></td>
						<td><span ng-bind="row.type"></td>
						<td><span ng-bind="row.host"></td>
						<td><span ng-bind="row.port"></td>
						<td>
							<button type="button" ng-click="update_endpoint()" class="btn btn-xs btn-warning" style="text-align: center;">修改</button>
							<button type="button" ng-click="del_endpoint(row.id)" class="btn btn-xs btn-danger" style="text-align: center;">删除</button>
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
		<div class="modal fade" id="add-endpoint-modal" tabindex="-1" role="dialog" aria-labelledby="add-endpoint-label" aria-hidden="true">
			<div class="modal-dialog" style="width: 400px">
				<div class="modal-content">
					<div class="modal-header">
						<button type="button" class="close" data-dismiss="modal" aria-label="Close"><span aria-hidden="true">&times;</span></button>
						<h4 class="modal-title" id="add-consumer-label">新增 Endpoint</h4>
					</div>
					<div class="modal-body">
						<form class="form-horizontal">
							<div class="form-group">
								<label for="inputEndpointName" class="col-sm-3 control-label">名称</label>
								<div class="col-sm-9">
									<input class="form-control" id="inputEndpointName" placeholder="Name" ng-model="new_endpoint.id">
								</div>
							</div>
							<div class="form-group">
								<label for="inputEndpointType" class="col-sm-3 control-label">类型</label>
								<div class="col-sm-9">
									<input type="text" class="form-control" id="inputEndpointType" data-provide="typeahead" placeholder="Type" ng-model="new_endpoint.type">
								</div>
							</div>
							<div class="form-group">
								<label for="inputEndpointHost" class="col-sm-3 control-label">Host</label>
								<div class="col-sm-9">
									<input class="form-control" id="inputEndpointHost" placeholder="Host" ng-model="new_endpoint.host">
								</div>
							</div>
							<div class="form-group">
								<label for="inputEndpointPort" class="col-sm-3 control-label">Port</label>
								<div class="col-sm-9">
									<input type="number" class="form-control" id="inputEndpointPort" placeholder="Port" ng-model="new_endpoint.port">
								</div>
							</div>
						</form>
					</div>
					<div class="modal-footer">
						<button type="button" class="btn btn-sm btn-danger" data-dismiss="modal">关闭</button>
						<button type="button" class="btn btn-sm btn-success" data-dismiss="modal" ng-click="add_endpoint(new_endpoint)">保存</button>
					</div>
				</div>
			</div>
		</div>
	</div>

	<script type="text/javascript" src="${model.webapp}/js/angular.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/angular-resource.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/smart-table.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/bootbox.min.js"></script>

	<script type="text/javascript" src="${model.webapp}/js/endpoint.js"></script>
</a:layout>