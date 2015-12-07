<%@ page contentType="text/html; charset=utf-8" isELIgnored="false" trimDirectiveWhitespaces="true"%>
<%@ taglib prefix="a" uri="/WEB-INF/app.tld"%>
<jsp:useBean id="ctx" type="com.ctrip.hermes.portal.console.storage.Context" scope="request" />
<jsp:useBean id="payload" type="com.ctrip.hermes.portal.console.storage.Payload" scope="request" />
<jsp:useBean id="model" type="com.ctrip.hermes.portal.console.storage.Model" scope="request" />

<a:layout>
	<link href="${model.webapp}/css/xeditable.css" type="text/css" rel="stylesheet">
	<link href="${model.webapp}/css/dashboard.css" type="text/css" rel="stylesheet">

	<div class="container fluid" ng-app="hermes-storage" ng-controller="storage-controller">
		<div class="row">
			<div class="col-sm-3 col-md-2 sidebar">
				<ul class="nav nav-sidebar" role="tablist">
					<li ng-click="set_selected(type)" ng-repeat="type in storage_types" role="presentation" ng-class="$first ? 'active' : ''"><a href="#content" role="tab" data-toggle="tab"
						aria-controls="content"> <span ng-bind="type" style="text-transform: capitalize;"></span>
					</a></li>
				</ul>
			</div>
			<div id="content" role="tabpanel" class="main col-md-offset-2 tab-content col-md-10 tab-pane fade in active ">
				<div class="panel panel-primary">
					<div class="panel-heading">
						<label><span style="text-transform: capitalize;" ng-bind="selected.type"></span> Datasource 列表</label>

						<div style="float: right">
							<button type="button" data-toggle="modal" data-target="#add-datasource-modal" class="btn btn-sm btn-success" style="text-align: center; font-size: 12px">新增</button>
						</div>
					</div>
					<div class="panel-body">
						<div ng-repeat="ds in selected.datasources">
							<div class="panel panel-info">
								<div class="storage-heading">

									<div style="font-size: 20px; height: 34px">
										<span>ID: {{ds.id}}</span>
										<span ng-if="is_mysql(selected.type)">Size: {{ds.storage_size}}</span>
										<div style="float: right">
											<!-- <button type="button" class="btn btn-default" ng-if="is_mysql(selected.type)" ng-click="isShowAll()">显示所有表</button> -->
											<button class="btn btn-danger btn-sm" ng-click="del_datasource(ds)">删除</button>
										</div>
									</div>
									<!-- <storage id="ds.id" type="selected.type" ng-if="is_mysql(selected.type)"> </storage> -->
								</div>
								<table class="table table-condensed table-responsive table-bordered">
									<thead>
										<tr>
											<th style="width: 5%">#</th>
											<th>KEY</th>
											<th>VALUE</th>
											<th width="100px"><button class="btn btn-success btn-xs" ng-click="add_row(ds)"><span class="glyphicon glyphicon-plus"></span> 新增</button></th>
										</tr>
									</thead>
									<tbody>
										<tr ng-repeat="prop in ds.properties">
											<td style="width: 5%;"><label ng-bind="$index + 1"></label></td>
											<td><span editable-text="prop.name" e-name="name" e-form="rowform" ng-bind="prop.name || 'Not Set'"></span></td>
											<td><span editable-text="prop.value" e-name="value" e-form="rowform" ng-bind="prop.value || 'Not Set'"></span></td>
											<td width="150px">
												<form editable-form shown="inserted == prop" name="rowform" ng-show="rowform.$visible" class="form-buttons form-inline" onaftersave="update_datasource(ds, $data)">
													<button type="submit" ng-disabled="rowform.$waiting" class="btn btn-primary btn-xs"><span class="glyphicon glyphicon-floppy-save"></span> 保存</button>
													<button type="button" ng-disabled="rowform.$waiting" ng-click="rowform.$cancel()" class="btn btn-danger btn-xs"><span class="glyphicon glyphicon-log-out"></span> 取消</button>
												</form>
												<div class="buttons" ng-show="!rowform.$visible">
													<button class="btn btn-warning btn-xs" ng-click="rowform.$show()"><span class="glyphicon glyphicon-edit"></span> 修改</button>
													<button class="btn btn-danger btn-xs" ng-click="del_row(ds,prop.name)"><span class="glyphicon glyphicon-remove"></span> 删除</button>
												</div>
											</td>
										</tr>
									</tbody>
								</table>
							</div>
						</div>
					</div>
				</div>
			</div>
		</div>
		<%--add-datasource-modal--%>
		<div class="modal fade" id="add-datasource-modal" tabindex="-1" role="dialog" aria-labelledby="add-consumer-label" aria-hidden="true">
			<div class="modal-dialog">
				<div class="modal-content">
					<div class="modal-header">
						<button type="button" class="close" data-dismiss="modal" aria-label="Close"><span aria-hidden="true">&times;</span></button>
						<h4 class="modal-title" id="add-consumer-label">新增 DataSource On {{selected.type.toUpperCase() }}</h4>
					</div>
					<div class="modal-body">
						<form class="form-horizontal">
							<div class="form-group">
								<div class="col-sm-8" style="width: 30%; font-size: 14px; padding-left: 20px">KEY</div>
								<div class="col-sm-8" style="width: 60%; font-size: 14px; padding-left: 20px">Value</div>
								<button class="btn btn-success btn-sm" ng-click="add_kv($index)" style="width: 8%; font-size: 10px">增加</button>
							</div>
							<div class="form-group" ng-repeat="form in forms">

								<div class="col-sm-8" style="width: 30%">
									<input type="text" class="form-control" data-provide="typeahead" ng-model="form.key">
								</div>
								<div class="col-sm-8" style="width: 60%">
									<input type="text" class="form-control" data-provide="typeahead" placeholder="{{form.placeholder}}" ng-model="form.value">
								</div>
								<button class="btn btn-danger btn-sm" ng-click="del_kv($index)" style="width: 8%; font-size: 10px">删除</button>

							</div>
						</form>
					</div>
					<div class="modal-footer">
						<button type="button" class="btn btn-sm btn-warning" ng-click="reset()">重置</button>
						<button type="button" class="btn btn-sm btn-success" data-dismiss="modal" ng-click="add_datasource()">保存</button>
					</div>
				</div>
			</div>
		</div>

	</div>

	<script type="text/javascript" src="${model.webapp}/js/angular/angular-strap.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/angular/angular-strap.tpl.min.js"></script>
	
	<script type="text/javascript" src="${model.webapp}/js/angular/xeditable.min.js"></script>

	<script type="text/javascript" src="${model.webapp}/js/storage/storage.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/storage/service.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/storage/controller.js"></script>
</a:layout>