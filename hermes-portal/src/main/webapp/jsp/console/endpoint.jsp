<%@ page contentType="text/html; charset=utf-8" isELIgnored="false" trimDirectiveWhitespaces="true"%>
<%@ taglib prefix="a" uri="/WEB-INF/app.tld"%>
<jsp:useBean id="ctx" type="com.ctrip.hermes.portal.console.endpoint.Context" scope="request" />
<jsp:useBean id="payload" type="com.ctrip.hermes.portal.console.endpoint.Payload" scope="request" />
<jsp:useBean id="model" type="com.ctrip.hermes.portal.console.endpoint.Model" scope="request" />

<a:layout>
	<link href="${model.webapp}/css/xeditable.css" type="text/css" rel="stylesheet">
	<div class="container" ng-app="hermes-endpoint" ng-controller="endpoint-controller" style="margin-top: 20px">
		<div class="row">
		<ul class="nav nav-tabs col-md-8">
			<li ng-repeat="groupType in groupTypes" role="presentation" ng-class="{active:groupType==currentGroupType}"><a href="#" ng-click="switchCurrentGroupType(groupType)">{{groupType}} <span class="badge">{{groupTypeToEndpointMap[groupType].length}}</span></a></li>
		</ul>

		<form class="form-inline col-md-3 col-md-offset-1" role="form">
			<div class="form-group">
				<input id="searchHost" ng-model="searchHost" type="text" class="form-control" placeholder="Search: Accurate Host" >
			</div>
			<button type="button" class="btn btn-sm btn-success" ng-click="searchByip(searchHost)">GO!</button>
		</form>
		</div>



		<table class="table table-striped" style="font-size: small; margin-top: 20px" st-table="displayedEndpoints" st-safe-src="currentEndpoints">
			<thead>
				<tr>
					<th>#</th>
					<th st-sort="id">名称</th>
					<th st-sort="id">类型</th>
					<th st-sort="host">Host</th>
					<th st-sort="port">Port</th>
					<th st-sort="enabled">Enabled</th>
					<th st-sort="enabled">Idc</th>
				</tr>
				<tr>
					<th></th>
					<th><input st-search="id" placeholder="Name" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
					<th><input st-search="type" placeholder="Type" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
					<th><input st-search="host" placeholder="Host" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
					<th><input st-search="port" placeholder="Port" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
					<th><input st-search="enabled" placeholder="Enabled" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
					<th><input st-search="idc" placeholder="Idc" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
				</tr>
			</thead>
			<tbody>
				<tr ng-if="currentEndpoints.length!=0" ng-repeat="endpoint in displayedEndpoints" ng-style="currentEndpoint.host==endpoint.host&&{'border':'3px solid #E07A7A'}">
					<td>{{$index+1}}</td>
					<td>{{endpoint.id}}</td>
					<td>{{endpoint.type}}</td>
					<td>{{endpoint.host}}</td>
					<td>{{endpoint.port}}</td>
					<td>{{endpoint.enabled}}</td>
					<td>{{endpoint.idc}}</td>
					<td ng-if="currentGroupType!=idleGroupType" style="white-space: nowrap">
						<button class="btn btn-danger btn-xs" data-toggle="modal" data-target="#offline" ng-click="setCurrentEndpoint(endpoint)">下线</button>
					</td>
					<td ng-if="currentGroupType==idleGroupType" style="white-space: nowrap">
						<button class="btn btn-primary btn-xs" data-toggle="modal" data-target="#online" ng-click="setCurrentEndpoint(endpoint)">上线</button>
					</td>
				</tr>
				<tr ng-if="currentEndpoints.length==0">
					<td colspan="8"><label>No Endpoints Found!</label></td>
				</tr>
			</tbody>
		</table>

		<div class="modal fade" id="offline" tabindex="-1" role="dialog" style="margin-top: 100px;">
			<div class="modal-dialog" style="width: 400px">
				<div class="modal-content">
					<div class="modal-body">
						<h5>
							确认要下线机器: <strong>{{currentEndpoint.id}}</strong> (ip: {{currentEndpoint.host}})？
						</h5>
					</div>
					<div class="modal-footer">
						<button type="button" class="btn btn-sm btn-danger" data-dismiss="modal">取消</button>
						<button type="button" class="btn btn-sm btn-success" data-dismiss="modal" ng-click="offline(currentEndpoint)">确认</button>
					</div>
				</div>
			</div>
		</div>

		<div class="modal fade" id="online" tabindex="-1" role="dialog" style="margin-top: 100px;">
			<div class="modal-dialog" style="width: 500px">
				<div class="modal-content">
					<div class="modal-body">
						<form class="form-inline">
							<span><strong>{{currentEndpoint.id}}</strong> ({{currentEndpoint.host}})</span>
							<div class="form-group">
								<input id="groupType" type="text" class="form-control" placeholder="Group" ng-model="targetGroupType">
							</div>
							<button type="button" class="btn btn-sm btn-danger" data-dismiss="modal">取消</button>
							<button type="button" class="btn btn-sm btn-success" data-dismiss="modal" ng-click="online(currentEndpoint,targetGroupType)">上线</button>
						</form>
					</div>
				</div>
			</div>
		</div>
	</div>

	<script type="text/javascript" src="${model.webapp}/js/angular/smart-table.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/endpoint/endpoint.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/lib/global.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/lib/utils.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/lib/components.js"></script>
</a:layout>