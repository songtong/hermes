<%@ page contentType="text/html; charset=utf-8" isELIgnored="false" trimDirectiveWhitespaces="true"%>
<%@ taglib prefix="a" uri="/WEB-INF/app.tld"%>
<jsp:useBean id="ctx" type="com.ctrip.hermes.portal.console.idc.Context" scope="request" />
<jsp:useBean id="payload" type="com.ctrip.hermes.portal.console.idc.Payload" scope="request" />
<jsp:useBean id="model" type="com.ctrip.hermes.portal.console.idc.Model" scope="request" />

<a:layout>
	<link href="${model.webapp}/css/xeditable.css" type="text/css" rel="stylesheet">
	<link href="${model.webapp}/css/angular-toggle-switch-bootstrap-3.css" type="text/css" rel="stylesheet">
	<div class="container" ng-app="idcApp" ng-controller="idcController" style="margin-top: 20px">
		<ul class="nav nav-tabs">
			<li ng-repeat="idc in idcs" role="presentation" ng-class="{active:currentIdc.id==idc.id}"><a href="#" ng-click="switchCurrentIdc(idc)">{{idc.name}} <span ng-show="idc.primary" class="glyphicon glyphicon-star" style="color: #DC0F00;"></span> <span ng-show="idc.enabled" class="glyphicon glyphicon-ok-sign" style="color: green;"></span></a></li>
			<li><a href="#" data-toggle="modal" data-target="#addIdcModal"><span class="glyphicon glyphicon-plus" style="border-bottom: 0px"></span></a></li>
		</ul>

		<form class="form-inline" style="margin-top: 20px">
			<div class="form-group">
				<label>Primary</label>
				<toggle-switch on-label="true" off-label="false" ng-model="currentIdc.primary" on-change="switchPrimary"> </toggle-switch>
			</div>
			<div class="form-group" style="margin-left: 20px">
				<label>Enabled</label>
				<toggle-switch on-label="true" off-label="false" ng-model="currentIdc.enabled" on-change="switchEnabled"> </toggle-switch>
			</div>
			<button style="margin-left: 20px" class="btn btn-danger" ng-click="deleteIdc()">Delete</button>
		</form>

		<hr>
		<h4>
			servers <span class="badge">{{currentServers.length}}</span>
			<button type="button" class="btn btn-success btn-xs" data-toggle="modal" data-target="#addServerModal">
				<span class="glyphicon glyphicon-plus"></span>
			</button>
		</h4>
		<table class="table table-striped" style="font-size: small;" st-table="displayedServers" st-safe-src="currentServers">
			<thead>
				<tr>
					<th>#</th>
					<th st-sort="id">名称</th>
					<th st-sort="host">Host</th>
					<th st-sort="port">Port</th>
					<th st-sort="enabled">Enabled</th>
				</tr>
				<tr>
					<th></th>
					<th><input st-search="id" placeholder="Name" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
					<th><input st-search="host" placeholder="Host" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
					<th><input st-search="port" placeholder="Port" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
					<th><input st-search="enabled" placeholder="Enabled" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
				</tr>
			</thead>
			<tbody>
				<tr ng-if="currentServers.length!=0" ng-repeat="server in displayedServers">
					<td>{{$index+1}}</td>
					<td><span e-form="serverForm" e-name="id">{{server.id}}</span></td>
					<td><span e-form="serverForm" e-name="host" editable-text="server.host">{{server.host}}</span></td>
					<td><span e-form="serverForm" e-name="port" editable-text="server.port">{{server.port}}</span></td>
					<td><span e-form="serverForm" e-name="enabled" editable-select="server.enabled" e-ng-options="op for op in ops">{{server.enabled}}</span></td>
					<td style="white-space: nowrap">
						<form editable-form name="serverForm" onaftersave="updateServer(server)" ng-show="serverForm.$visible" class="form-buttons form-inline">
							<button type="submit" ng-disabled="serverForm.$waiting" class="btn btn-primary btn-xs">save</button>
							<button type="button" ng-disabled="serverForm.$waiting" ng-click="serverForm.$cancel()" class="btn btn-default btn-xs">cancel</button>
						</form>
						<div class="buttons" ng-show="!serverForm.$visible">
							<button class="btn btn-primary btn-xs" ng-click="serverForm.$show()">edit</button>
							<button class="btn btn-danger btn-xs" ng-click="deleteServer(server.id)">del</button>
						</div>
					</td>
				</tr>
				<tr ng-if="currentServers.length==0">
					<td colspan="6"><label>No Server Found!</label></td>
				</tr>
			</tbody>
		</table>

		<hr>
		<h4>
			endpoints <span class="badge">{{currentEndpoints.length}}</span>
			<button type="button" class="btn btn-success btn-xs" data-toggle="modal" data-target="#addEndpointModal">
				<span class="glyphicon glyphicon-plus"></span>
			</button>
		</h4>
		<table class="table table-striped" style="font-size: small;" st-table="displayedCurrentEndpoints" st-safe-src="currentEndpoints">
			<thead>
				<tr>
					<th>#</th>
					<th st-sort="id">名称</th>
					<th st-sort="type">类型</th>
					<th st-sort="host">Host</th>
					<th st-sort="port">Port</th>
					<th st-sort="enabled">Enabled</th>
					<th st-sort="group">group</th>
				</tr>
				<tr>
					<th></th>
					<th><input st-search="id" placeholder="Name" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
					<th><input st-search="type" placeholder="Type" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
					<th><input st-search="host" placeholder="Host" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
					<th><input st-search="port" placeholder="Port" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
					<th><input st-search="enabled" placeholder="Enabled" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
					<th><input st-search="group" placeholder="Group" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
				</tr>
			</thead>
			<tbody>
				<tr ng-if="currentEndpoints.length!=0" ng-repeat="endpoint in displayedCurrentEndpoints">
					<td>{{$index+1}}</td>
					<td><span e-form="endpointForm" e-name="id">{{endpoint.id}}</span></td>
					<td><span e-form="endpointForm" e-name="type">{{endpoint.type}}</span></td>
					<td><span e-form="endpointForm" e-name="host" editable-text="endpoint.host">{{endpoint.host}}</span></td>
					<td><span e-form="endpointForm" e-name="port" editable-text="endpoint.port">{{endpoint.port}}</span></td>
					<td><span e-form="endpointForm" e-name="enabled" editable-select="endpoint.enabled" e-ng-options="op for op in ops">{{endpoint.enabled}}</span></td>
					<td><span>{{endpoint.group}}</span></td>
					<td style="white-space: nowrap">
						<form editable-form name="endpointForm" onaftersave="updateEndpoint(endpoint)" ng-show="endpointForm.$visible" class="form-buttons form-inline">
							<button type="submit" ng-disabled="endpointForm.$waiting" class="btn btn-primary btn-xs">save</button>
							<button type="button" ng-disabled="endpointForm.$waiting" ng-click="endpointForm.$cancel()" class="btn btn-default btn-xs">cancel</button>
						</form>
						<div class="buttons" ng-show="!endpointForm.$visible">
							<button class="btn btn-primary btn-xs" ng-click="endpointForm.$show()">edit</button>
							<button ng-class="{disabled:endpoint.group!='idle'}" class="btn btn-danger btn-xs" ng-click="deleteEndpoint(endpoint.id)">del</button>
						</div>
					</td>
				</tr>
				<tr ng-if="currentEndpoints.length==0">
					<td colspan="5"><label>No Endpoint Found!</label></td>
				</tr>
			</tbody>
		</table>

		<div class="modal fade" id="addIdcModal" tabindex="-1" role="dialog">
			<div class="modal-dialog" style="width: 400px">
				<div class="modal-content">
					<div class="modal-header">
						<button type="button" class="close" data-dismiss="modal" aria-label="Close">
							<span aria-hidden="true">&times;</span>
						</button>
						<h4 class="modal-title">新增 Idc</h4>
					</div>
					<div class="modal-body">
						<form class="form-horizontal">
							<div class="form-group">
								<label class="col-sm-3 control-label">名称</label>
								<div class="col-sm-9">
									<input class="form-control" placeholder="ID" ng-model="newIdc.name">
								</div>
							</div>
							<div class="form-group">
								<label class="col-sm-3 control-label">启用？</label>
								<div class="col-sm-9">
									<select class="form-control" placeholder="Enabled" ng-model="newIdc.enabled" ng-options="op for op in ops"></select>
								</div>
							</div>
							<div class="form-group">
								<label class="col-sm-3 control-label">primary？</label>
								<div class="col-sm-9">
									<p class="form-control-static" placeholder="primary" ng-bind="newIdc.primary"></p>
								</div>
							</div>
						</form>
					</div>
					<div class="modal-footer">
						<button type="button" class="btn btn-sm btn-danger" data-dismiss="modal">关闭</button>
						<button type="button" class="btn btn-sm btn-success" data-dismiss="modal" ng-click="addIdc(newIdc)">保存</button>
					</div>
				</div>
			</div>
		</div>

		<div class="modal fade" id="addServerModal" tabindex="-1" role="dialog">
			<div class="modal-dialog" style="width: 400px">
				<div class="modal-content">
					<div class="modal-header">
						<button type="button" class="close" data-dismiss="modal" aria-label="Close">
							<span aria-hidden="true">&times;</span>
						</button>
						<h4 class="modal-title">新增 Server</h4>
					</div>
					<div class="modal-body">
						<form class="form-horizontal">
							<div class="form-group">
								<label class="col-sm-3 control-label">Host</label>
								<div class="col-sm-9">
									<input class="form-control" placeholder="Host" ng-model="newServer.host" ng-blur="newServer.id=newServer.idc.toLowerCase()+'-'+newServer.host">
								</div>
							</div>
							<div class="form-group">
								<label class="col-sm-3 control-label">名称</label>
								<div class="col-sm-9">
									<input class="form-control" placeholder="Name" ng-model="newServer.id">
								</div>
							</div>
							<div class="form-group">
								<label class="col-sm-3 control-label">Port</label>
								<div class="col-sm-9">
									<input type="number" class="form-control" placeholder="Port" ng-model="newServer.port">
								</div>
							</div>
							<div class="form-group">
								<label class="col-sm-3 control-label">Idc</label>
								<div class="col-sm-9">
									<p class="form-control-static" placeholder="Idc" ng-bind="newServer.idc"></p>
								</div>
							</div>
						</form>
					</div>
					<div class="modal-footer">
						<button type="button" class="btn btn-sm btn-danger" data-dismiss="modal">关闭</button>
						<button type="button" class="btn btn-sm btn-success" data-dismiss="modal" ng-click="addServer(newServer)">保存</button>
					</div>
				</div>
			</div>
		</div>

		<div class="modal fade" id="addEndpointModal" tabindex="-1" role="dialog">
			<div class="modal-dialog" style="width: 400px">
				<div class="modal-content">
					<div class="modal-header">
						<button type="button" class="close" data-dismiss="modal" aria-label="Close">
							<span aria-hidden="true">&times;</span>
						</button>
						<h4 class="modal-title">新增 Endpoint</h4>
					</div>
					<div class="modal-body">
						<form class="form-horizontal">
							<div class="form-group">
								<label for="inputEndpointHost" class="col-sm-3 control-label">Host</label>
								<div class="col-sm-9">
									<input class="form-control" id="inputEndpointHost" placeholder="Host" ng-model="newEndpoint.host" ng-blur="newEndpoint.id=newEndpoint.idc.toLowerCase()+'-'+newEndpoint.host">
								</div>
							</div>
							<div class="form-group">
								<label for="inputEndpointName" class="col-sm-3 control-label">名称</label>
								<div class="col-sm-9">
									<input class="form-control" id="inputEndpointName" placeholder="Name" ng-model="newEndpoint.id">
								</div>
							</div>
							<div class="form-group">
								<label for="inputEndpointType" class="col-sm-3 control-label">类型</label>
								<div class="col-sm-9">
									<select class="form-control" id="inputEndpointType" placeholder="Type" ng-model="newEndpoint.type" ng-options="type for type in endpointsTypes"></select>
								</div>
							</div>
							<div class="form-group">
								<label for="inputEndpointPort" class="col-sm-3 control-label">Port</label>
								<div class="col-sm-9">
									<input type="number" class="form-control" id="inputEndpointPort" placeholder="Port" ng-model="newEndpoint.port">
								</div>
							</div>
							<div class="form-group">
								<label for="inputEndpointGroup" class="col-sm-3 control-label">Group</label>
								<div class="col-sm-9">
									<p class="form-control-static" id="inputEndpointGroup" placeholder="Group" ng-bind="newEndpoint.group"></p>
								</div>
							</div>
							<div class="form-group">
								<label for="inputEndpointIdc" class="col-sm-3 control-label">Idc</label>
								<div class="col-sm-9">
									<p class="form-control-static" id="inputEndpointIdc" placeholder="Idc" ng-bind="newEndpoint.idc"></p>
								</div>
							</div>
						</form>
					</div>
					<div class="modal-footer">
						<button type="button" class="btn btn-sm btn-danger" data-dismiss="modal">关闭</button>
						<button type="button" class="btn btn-sm btn-success" data-dismiss="modal" ng-click="addEndpoint(newEndpoint)">保存</button>
					</div>
				</div>
			</div>
		</div>
	</div>



	<script type="text/javascript" src="${model.webapp}/js/idc/idc.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/angular/xeditable.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/angular/smart-table.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/angular-toggle-switch.min.js"></script>
</a:layout>