<%@ page contentType="text/html; charset=utf-8" isELIgnored="false" trimDirectiveWhitespaces="true"%>
<%@ taglib prefix="a" uri="/WEB-INF/app.tld"%>
<jsp:useBean id="ctx" type="com.ctrip.hermes.portal.console.storage.Context" scope="request" />
<jsp:useBean id="payload" type="com.ctrip.hermes.portal.console.storage.Payload" scope="request" />
<jsp:useBean id="model" type="com.ctrip.hermes.portal.console.storage.Model" scope="request" />

<a:layout>
	<link href="${model.webapp}/css/xeditable.css" type="text/css" rel="stylesheet">
	<div class="op-alert alert alert-info" role="alert" style="display: none;">
		<span>The examples populate this alert with dummy content</span>
	</div>
	<div class="row" ng-app="hermes-storage" ng-controller="storage-controller">
		<div class="col-md-2">
			<ul class="nav nav-pills nav-stacked" role="tablist">
				<li ng-click="set_selected(type)" ng-repeat="type in storage_types" role="presentation" ng-class="$first ? 'active' : ''"><a href="#content" role="tab" data-toggle="tab"
						aria-controls="content">
						<span ng-bind="type" style="text-transform: capitalize;"></span>
					</a></li>
			</ul>
		</div>
		<div id="content" role="tabpanel" class="tab-content col-md-10 tab-pane fade in active">
			<div class="panel panel-primary">
				<div class="panel-heading">
					<label><span style="text-transform: capitalize;" ng-bind="selected.type"></span> Datasource 列表</label>
				</div>
				<div class="panel-body">
					<div ng-repeat="ds in selected.datasources">
						<div class="panel panel-info">
							<div class="panel-heading">
								<span ng-bind="ds.id"></span>
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

	<script type="text/javascript" src="${model.webapp}/js/angular.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/angular-resource.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/bootbox.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/xeditable.min.js"></script>

	<script type="text/javascript" src="${model.webapp}/js/storage.js"></script>
</a:layout>