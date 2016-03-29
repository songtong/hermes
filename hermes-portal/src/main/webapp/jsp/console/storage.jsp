<%@ page contentType="text/html; charset=utf-8" isELIgnored="false" trimDirectiveWhitespaces="true"%>
<%@ taglib prefix="a" uri="/WEB-INF/app.tld"%>
<jsp:useBean id="ctx" type="com.ctrip.hermes.portal.console.storage.Context" scope="request" />
<jsp:useBean id="payload" type="com.ctrip.hermes.portal.console.storage.Payload" scope="request" />
<jsp:useBean id="model" type="com.ctrip.hermes.portal.console.storage.Model" scope="request" />

<a:layout>
	<link href="${model.webapp}/css/xeditable.css" type="text/css" rel="stylesheet">
	<link href="${model.webapp}/css/dashboard.css" type="text/css" rel="stylesheet">
	<link href="${model.webapp}/css/select2.min.css" type="text/css" rel="stylesheet">
	<link href="${model.webapp}/css/font-awesome.min.css" type="text/css" rel="stylesheet">

	<div class="main fluid" ng-app="hermes-storage" ng-controller="storage-controller">
			<div class="col-sm-3 col-md-2 sidebar">
				<ul class="nav nav-sidebar" role="tablist">
					<li ng-click="selectStorage(ds.type)" ng-repeat="ds in __datasources" role="presentation" ng-class="$first ? 'active' : ''"><a href="#content" role="tab" data-toggle="tab"
						aria-controls="content"> <span ng-bind="ds.type" style="text-transform: capitalize;"></span>
					</a></li>
				</ul>
			</div>
			<div class="col-md-offset-2 col-md-10">
				<div class="panel panel-primary">
					<div class="panel-heading">
						<label><span style="text-transform: capitalize;"></span> DataSource 列表</label>
						<div style="float: right">
							<button type="button" data-toggle="modal" data-target="#datasource-modal" class="btn btn-sm btn-success" style="text-align: center; font-size: 12px" ng-click="add()">新增</button>
						</div>
					</div>
					<div class="panel-body" refresh-x ng-if="datasources" ng-include="'template/storage/templates/' + storageType +'.html'">
					</div>
				</div>
			</div>

			<%--add-datasource-modal--%>
			<div class="modal fade" id="datasource-modal" tabindex="-1" role="dialog" aria-labelledby="datasource-label" aria-hidden="true">
				<div class="modal-dialog">
					<div class="modal-content">
						<div class="modal-header">
							<button type="button" class="close" data-dismiss="modal" aria-label="Close"><span aria-hidden="true">&times;</span></button>
							<h4 class="modal-title" id="datasource-label" ng-if="currentDatasource">DataSource [{{currentDatasource.id | uppercase}}]</h4>
						</div>
						<div class="modal-body" ng-include="'template/storage/templates/' + storageType +'-form.html'">
						</div>
						<div class="modal-footer">
							<button type="button" class="btn btn-sm btn-warning" ng-click="reset($event)">重置</button>
							<button type="button" class="btn btn-sm btn-success" data-dismiss="modal" ng-click="save()">保存</button>
						</div>
					</div>
				</div>
			</div>
			<loading-x></loading-x>
			
			<confirm-dialog-x id="confirmDialog" title="确认" content="确认要删除此数据源？" action="remove"></confirm-dialog-x>
			
			<progressbar-x id="progressBar"/></progressbar-x>
	</div>
	<script type="text/javascript" src="${model.webapp}/js/angular/angular-strap.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/angular/angular-strap.tpl.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/angular/xeditable.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/angular/smart-table.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/select2.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/bootstrap-tagsinput.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/bootstrap-tagsinput-angular.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/lib/global.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/lib/utils.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/lib/components.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/storage/storage.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/storage/service.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/storage/controller.js"></script>

</a:layout>