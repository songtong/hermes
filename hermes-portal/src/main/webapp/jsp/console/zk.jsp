<%@ page contentType="text/html; charset=utf-8" isELIgnored="false" trimDirectiveWhitespaces="true"%>
<%@ taglib prefix="a" uri="/WEB-INF/app.tld"%>
<jsp:useBean id="ctx" type="com.ctrip.hermes.portal.console.zk.Context" scope="request" />
<jsp:useBean id="payload" type="com.ctrip.hermes.portal.console.zk.Payload" scope="request" />
<jsp:useBean id="model" type="com.ctrip.hermes.portal.console.zk.Model" scope="request" />

<a:layout>
	<link href="${model.webapp}/css/xeditable.css" type="text/css" rel="stylesheet">
	<link href="${model.webapp}/css/bootstrap-multistep.css" type="text/css" rel="stylesheet">
	<div class="container" ng-app="zkApp" ng-controller="zkController" style="margin-top: 20px">
		<ul class="nav nav-tabs">
			<li role="presentation" ng-class="{active:currentPage=='zookeeperEnsembles'}"><a href="#" ng-click="currentPage='zookeeperEnsembles'">集群管理 </a></li>
			<li role="presentation" ng-class="{active:currentPage=='zookeeperMigration'}"><a href="#" ng-click="currentPage='zookeeperMigration'">ZK迁移 </a></li>
		</ul>

		<!-- zookeeper ensembles management view -->
		<table ng-show="currentPage=='zookeeperEnsembles'" class="table table-striped" style="font-size: small;" st-table="displayedZookeeperEnsembles" st-safe-src="zookeeperEnsembles">
			<thead>
				<tr>
					<th>#</th>
					<th st-sort="name">名称</th>
					<th st-sort="connectionString">连接串</th>
					<th st-sort="idc">Idc</th>
					<th st-sort="primary">优先</th>
					<th><a href="#" class="btn btn-xs btn-success" data-toggle="modal" data-target="#addZookeeperEnsembleModel"> <span class="glyphicon glyphicon-plus"></span> 新增
					</a></th>
				</tr>
				<tr>
					<th></th>
					<th><input st-search="name" placeholder="Name" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
					<th><input st-search="connectionString" placeholder="ConnectionString" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
					<th><input st-search="idc" placeholder="Idc" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
					<th><input st-search="primary" placeholder="Primary" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
				</tr>
			</thead>
			<tbody>
				<tr ng-if="zookeeperEnsembles.length!=0" ng-repeat="zookeeperEnsemble in displayedZookeeperEnsembles">
					<td>{{$index+1}}</td>
					<td><span e-form="zookeeperEnsembleForm" e-name="name">{{zookeeperEnsemble.name}}</span></td>
					<td><span e-form="zookeeperEnsembleForm" e-name="connectionString" editable-text="zookeeperEnsemble.connectionString">{{zookeeperEnsemble.connectionString}}</span></td>
					<td><span e-form="zookeeperEnsembleForm" e-name="idc" editable-text="zookeeperEnsemble.idc">{{zookeeperEnsemble.idc}}</span></td>
					<td><span e-form="zookeeperEnsembleForm" e-name="primary">{{zookeeperEnsemble.primary}}</span></td>
					<td style="white-space: nowrap">
						<form editable-form name="zookeeperEnsembleForm" onaftersave="updatezookeeperEnsemble(zookeeperEnsemble)" ng-show="zookeeperEnsembleForm.$visible" class="form-buttons form-inline">
							<button type="submit" ng-disabled="zookeeperEnsembleForm.$waiting" class="btn btn-primary btn-xs">save</button>
							<button type="button" ng-disabled="zookeeperEnsembleForm.$waiting" ng-click="zookeeperEnsembleForm.$cancel()" class="btn btn-default btn-xs">cancel</button>
						</form>
						<div class="buttons" ng-show="!zookeeperEnsembleForm.$visible">
							<button class="btn btn-primary btn-xs" ng-click="zookeeperEnsembleForm.$show()" ng-class="{disabled:zookeeperEnsemble.primary}">edit</button>
							<button class="btn btn-danger btn-xs" ng-click="deletezookeeperEnsemble(zookeeperEnsemble)" ng-class="{disabled:zookeeperEnsemble.primary}">del</button>
						</div>
					</td>
				</tr>
				<tr ng-if="zookeeperEnsembles.length==0">
					<td colspan="6"><label>No ZookeeperEnsemble Found!</label></td>
				</tr>
			</tbody>
		</table>

		<div class="modal fade" id="addZookeeperEnsembleModel" tabindex="-1" role="dialog">
			<div class="modal-dialog" style="width: 600px">
				<div class="modal-content">
					<div class="modal-header">
						<button type="button" class="close" data-dismiss="modal" aria-label="Close">
							<span aria-hidden="true">&times;</span>
						</button>
						<h4 class="modal-title">新增 ZookeeperEnsemble</h4>
					</div>
					<div class="modal-body">
						<form class="form-horizontal">
							<div class="form-group">
								<label class="col-sm-2 control-label">名称</label>
								<div class="col-sm-9">
									<input class="form-control" placeholder="name" ng-model="newZookeeperEnsemble.name">
								</div>
							</div>
							<div class="form-group">
								<label class="col-sm-2 control-label">连接串</label>
								<div class="col-sm-9">
									<input class="form-control" placeholder="connectionString" ng-model="newZookeeperEnsemble.connectionString">
								</div>
							</div>
							<div class="form-group">
								<label class="col-sm-2 control-label">Idc</label>
								<div class="col-sm-9">
									<input class="form-control" placeholder="idc" ng-model="newZookeeperEnsemble.idc">
								</div>
							</div>
							<div class="form-group">
								<label class="col-sm-2 control-label">primary</label>
								<div class="col-sm-9">
									<p class="form-control-static" placeholder="primary" ng-bind="newZookeeperEnsemble.primary"></p>
								</div>
							</div>
						</form>
					</div>
					<div class="modal-footer">
						<button type="button" class="btn btn-sm btn-danger" data-dismiss="modal">关闭</button>
						<button type="button" class="btn btn-sm btn-success" data-dismiss="modal" ng-click="addzookeeperEnsemble(newZookeeperEnsemble)">保存</button>
					</div>
				</div>
			</div>
		</div>

		<!-- zookeeper migration view -->
		<div ng-show="currentPage=='zookeeperMigration'" style="margin-top: 20px">
			<div class="stepwizard">
				<div class="stepwizard-row setup-panel">
					<div class="stepwizard-step">
						<a href="#step-1" type="button" class="btn btn-primary btn-circle">1</a>
						<p>初始化zk</p>
					</div>
					<div class="stepwizard-step">
						<a href="#step-2" type="button" class="btn btn-default btn-circle" disabled="disabled">2</a>
						<p>停止租约分配</p>
					</div>
					<div class="stepwizard-step">
						<a href="#step-3" type="button" class="btn btn-default btn-circle" disabled="disabled">3</a>
						<p>更换Primary Zk</p>
					</div>
					<div class="stepwizard-step">
						<a href="#step-4" type="button" class="btn btn-default btn-circle" disabled="disabled">4</a>
						<p>切换zk（断连）</p>
					</div>
					<div class="stepwizard-step">
						<a href="#step-5" type="button" class="btn btn-default btn-circle" disabled="disabled">5</a>
						<p>切换zk（重连）</p>
					</div>
					<div class="stepwizard-step">
						<a href="#step-6" type="button" class="btn btn-default btn-circle" disabled="disabled">6</a>
						<p>启用租约分配</p>
					</div>
					<div class="stepwizard-step">
						<a href="#step-7" type="button" class="btn btn-default btn-circle" disabled="disabled">7</a>
						<p>完成</p>
					</div>
				</div>
			</div>

			<form style="margin-top: 20px;">
				<div class="row setup-content" id="step-1">
					<div class="col-md-8">
						<div class="col-md-12">
							<div class="form-group">
								<label class="control-label">当前zookeeper ensemble:</label> <input type="text" class="form-control" value="{{currentZookeeperEnsemble.name}}({{currentZookeeperEnsemble.idc}}, {{currentZookeeperEnsemble.connectionString}})" disabled />
							</div>
							<div class="form-group">
								<label class="control-label">请选择目标的zookeeper ensemble:</label> <select class="form-control" ng-model="target" ng-options="getZookeeperEnsembleLabel(zookeeperEnsemble) group by zookeeperEnsemble.idc for zookeeperEnsemble in notCurrentZookeeperEnsembles">
									<option value="" disabled></option>
								</select>
							</div>
							<div class="form-group">
								<span style="font-size: 18px; margin-top: 2px;" class="glyphicon glyphicon-repeat pull-right" ng-class="{rotate:initializeZk==checkStatus.checking}" ng-show="initializeZk==checkStatus.checking"></span>
								<button style="margin-right: 5px" class="btn btn-primary pull-right" type="button" ng-click="initlizeZookeeperEnsemble(target)">初始化</button>
							</div>
						</div>
					</div>
					<div class="col-md-3 col-md-offset-1">
						<h4>Step 1:初始化zk</h4>
						<button class="btn btn-primary nextBtn" type="button" ng-class="{disabled:!stepStatus.step1}">
							Next <span class="glyphicon glyphicon-chevron-right"></span>
						</button>
						<a href="#" class="row skipBtn"><em>Skip this step</em></a> <span class="glyphicon glyphicon-ok-sign" style="margin-left: 10px; color: #5cb85c; font-size: 20px;" ng-show="stepStatus.step1"></span>
						<p class="text-danger stepMessage" style="margin-top: 20px; word-wrap: break-word;"></p>
					</div>
				</div>
				<div class="row setup-content" id="step-2">
					<div class="col-md-8">
						<div class="col-md-12">
							<label>通知所有metaServer</label>
							<button style="margin-left: 5px" class="btn btn-danger" ng-click="stopLeaseAssignings()">通知</button>
							<label style="margin-left: 20px">通知所有<span class="text-danger">fail</span>的metaServer
							</label>
							<button style="margin-left: 5px" class="btn btn-danger" ng-click="stopLeaseAssigningsForFaildServers()">通知</button>
							<table class="table table-bordered" style="margin-top: 20px">
								<tr ng-repeat="metaServerRow in metaServers">
									<td ng-repeat="metaServer in metaServerRow track by $index" id="{{metaServer.id}}"><button class="btn" style="padding: 0px 2px;" tooltip="通知" ng-click=stopLeaseAssigning(metaServer)>
											<span class="glyphicon glyphicon-bell"></span>
										</button> {{metaServer.id}} <span class="glyphicon glyphicon-repeat" ng-class="{rotate:metaServer.stopLeaseAssigning==checkStatus.checking}" ng-show="metaServer.stopLeaseAssigning==checkStatus.checking"></span><span class="glyphicon glyphicon-ok-sign" style="color: #5cb85c;" ng-show="metaServer.stopLeaseAssigning==checkStatus.success"></span><span class="glyphicon glyphicon-remove-sign" style="color: #d9534f;" ng-show="metaServer.stopLeaseAssigning==checkStatus.fail"></span><span
										class="glyphicon glyphicon-question-sign" style="color: #f0ad4e;" ng-show="metaServer.stopLeaseAssigning==checkStatus.pedding"></span></td>
								</tr>
							</table>
						</div>
					</div>
					<div class="col-md-3 col-md-offset-1">
						<h4>Step 2:停止分配租约</h4>
						<button class="btn btn-primary nextBtn" type="button" ng-class="{disabled:!stepStatus.step2}">
							Next <span class="glyphicon glyphicon-chevron-right"></span>
						</button>
						<a href="#" class="row skipBtn"><em>Skip this step</em></a> <span class="glyphicon glyphicon-ok-sign" style="margin-left: 10px; color: #5cb85c; font-size: 20px;" ng-show="stepStatus.step2"></span>
						<p class="text-danger stepMessage" style="margin-top: 20px; word-wrap: break-word;"></p>
					</div>
				</div>
				<div class="row setup-content" id="step-3">
					<div class="col-md-8">
						<div class="col-md-12">
							<div class="form-group">
								<label class="control-label">当前zookeeper ensemble:</label> <input type="text" class="form-control" value="{{currentZookeeperEnsemble.name}}({{currentZookeeperEnsemble.idc}}, {{currentZookeeperEnsemble.connectionString}})" disabled />
							</div>
							<div class="form-group">
								<label class="control-label">目标zookeeper ensemble:</label> <input type="text" class="form-control" value="{{targetZookeeperEnsemble.name}}({{targetZookeeperEnsemble.idc}}, {{targetZookeeperEnsemble.connectionString}})" disabled />
							</div>
							<div class="form-group">
								<button class="btn btn-danger pull-right" type="button" ng-click="switchPrimaryZkToTarget()">更换</button>
							</div>
						</div>
					</div>
					<div class="col-md-3 col-md-offset-1">
						<h4>Step 3:更换Primary Zk</h4>
						<button class="btn btn-primary nextBtn" type="button" ng-class="{disabled:!stepStatus.step3}">
							Next <span class="glyphicon glyphicon-chevron-right"></span>
						</button>
						<a href="#" class="row skipBtn"><em>Skip this step</em></a> <span class="glyphicon glyphicon-ok-sign" style="margin-left: 10px; color: #5cb85c; font-size: 20px;" ng-show="stepStatus.step3"></span>
						<p class="text-danger stepMessage" style="margin-top: 20px; word-wrap: break-word;"></p>
					</div>
				</div>
				<div class="row setup-content" id="step-4">
					<div class="col-md-8">
						<div class="col-md-12">
							<label>通知portal</label>
							<button style="margin-left: 5px" class="btn btn-danger" ng-click="pauseAndSwitchZkForSelf()">通知</button>
							<label style="margin-left: 20px">通知所有metaServer</label>
							<button style="margin-left: 5px" class="btn btn-danger" ng-click="pauseAndSwitchZkForAllServers()">通知</button>
							<label style="margin-left: 20px">通知所有<span class="text-danger">fail</span>的metaServer
							</label>
							<button style="margin-left: 5px" class="btn btn-danger" ng-click="pauseAndSwitchZkForFailedServers()">通知</button>
							<table class="table table-bordered" style="margin-top: 20px">
								<tr>
									<td><button class="btn" style="padding: 0px 2px;" tooltip="通知" ng-click="pauseAndSwitchZkForSelf()">
											<span class="glyphicon glyphicon-bell"></span>
										</button> Portal <span class="glyphicon glyphicon-repeat" ng-class="{rotate:portal.disconnectToZk==checkStatus.checking}" ng-show="portal.disconnectToZk==checkStatus.checking"></span><span class="glyphicon glyphicon-ok-sign" style="color: #5cb85c;" ng-show="portal.disconnectToZk==checkStatus.success"></span><span class="glyphicon glyphicon-remove-sign" style="color: #d9534f;" ng-show="portal.disconnectToZk==checkStatus.fail"></span><span class="glyphicon glyphicon-question-sign"
										style="color: #f0ad4e;" ng-show="portal.disconnectToZk==checkStatus.pedding"></span></td>
								</tr>
							</table>
							<table class="table table-bordered" style="margin-top: 20px">
								<tr ng-repeat="metaServerRow in metaServers">
									<td ng-repeat="metaServer in metaServerRow track by $index" id="{{metaServer.id}}"><button class="btn" style="padding: 0px 2px;" tooltip="通知" ng-click="pauseAndSwitchZk(metaServer)">
											<span class="glyphicon glyphicon-bell"></span>
										</button> {{metaServer.id}} <span class="glyphicon glyphicon-repeat" ng-class="{rotate:metaServer.disconnectToZk==checkStatus.checking}" ng-show="metaServer.disconnectToZk==checkStatus.checking"></span><span class="glyphicon glyphicon-ok-sign" style="color: #5cb85c;" ng-show="metaServer.disconnectToZk==checkStatus.success"></span><span class="glyphicon glyphicon-remove-sign" style="color: #d9534f;" ng-show="metaServer.disconnectToZk==checkStatus.fail"></span><span
										class="glyphicon glyphicon-question-sign" style="color: #f0ad4e;" ng-show="metaServer.disconnectToZk==checkStatus.pedding"></span></td>
								</tr>
							</table>
						</div>
					</div>
					<div class="col-md-3 col-md-offset-1">
						<h4>Step 4:切换zk（断连）</h4>
						<button class="btn btn-primary nextBtn" type="button" ng-class="{disabled:!stepStatus.step4}">
							Next <span class="glyphicon glyphicon-chevron-right"></span>
						</button>
						<a href="#" class="row skipBtn"><em>Skip this step</em></a> <span class="glyphicon glyphicon-ok-sign" style="margin-left: 10px; color: #5cb85c; font-size: 20px;" ng-show="stepStatus.step4"></span>
						<p class="text-danger stepMessage" style="margin-top: 20px; word-wrap: break-word;"></p>
					</div>
				</div>
				<div class="row setup-content" id="step-5">
					<div class="col-md-8">
						<div class="col-md-12">
							<label>通知portal</label>
							<button style="margin-left: 5px" class="btn btn-danger" ng-click="resumeSelf()">通知</button>
							<label style="margin-left: 20px">通知所有metaServer</label>
							<button style="margin-left: 5px" class="btn btn-danger" ng-click="resumeAllServers()">通知</button>
							<label style="margin-left: 20px">通知所有<span class="text-danger">fail</span>的metaServer
							</label>
							<button style="margin-left: 5px" class="btn btn-danger" ng-click="resumeFailedServers()">通知</button>
							<table class="table table-bordered" style="margin-top: 20px">
								<tr>
									<td><button class="btn" style="padding: 0px 2px;" tooltip="通知" ng-click="resumeSelf()">
											<span class="glyphicon glyphicon-bell"></span>
										</button> Portal <span class="glyphicon glyphicon-repeat" ng-class="{rotate:portal.connectToZk==checkStatus.checking}" ng-show="portal.connectToZk==checkStatus.checking"></span><span class="glyphicon glyphicon-ok-sign" style="color: #5cb85c;" ng-show="portal.connectToZk==checkStatus.success"></span><span class="glyphicon glyphicon-remove-sign" style="color: #d9534f;" ng-show="portal.connectToZk==checkStatus.fail"></span><span class="glyphicon glyphicon-question-sign" style="color: #f0ad4e;"
										ng-show="portal.connectToZk==checkStatus.pedding"></span></td>
								</tr>
							</table>
							<table class="table table-bordered" style="margin-top: 20px">
								<tr ng-repeat="metaServerRow in metaServers">
									<td ng-repeat="metaServer in metaServerRow track by $index" id="{{metaServer.id}}"><button class="btn" style="padding: 0px 2px;" tooltip="通知" ng-click=resume(metaServer)>
											<span class="glyphicon glyphicon-bell"></span>
										</button> {{metaServer.id}} <span class="glyphicon glyphicon-repeat" ng-class="{rotate:metaServer.connectToZk==checkStatus.checking}" ng-show="metaServer.connectToZk==checkStatus.checking"></span><span class="glyphicon glyphicon-ok-sign" style="color: #5cb85c;" ng-show="metaServer.connectToZk==checkStatus.success"></span><span class="glyphicon glyphicon-remove-sign" style="color: #d9534f;" ng-show="metaServer.connectToZk==checkStatus.fail"></span><span class="glyphicon glyphicon-question-sign"
										style="color: #f0ad4e;" ng-show="metaServer.connectToZk==checkStatus.pedding"></span></td>
								</tr>
							</table>
						</div>
					</div>
					<div class="col-md-3 col-md-offset-1">
						<h4>Step 5:切换zk（重连）</h4>
						<button class="btn btn-primary nextBtn" type="button" ng-class="{disabled:!stepStatus.step5}">
							Next <span class="glyphicon glyphicon-chevron-right"></span>
						</button>
						<a href="#" class="row skipBtn"><em>Skip this step</em></a> <span class="glyphicon glyphicon-ok-sign" style="margin-left: 10px; color: #5cb85c; font-size: 20px;" ng-show="stepStatus.step5"></span>
						<p class="text-danger stepMessage" style="margin-top: 20px; word-wrap: break-word;"></p>
					</div>
				</div>
				<div class="row setup-content" id="step-6">
					<div class="col-md-8">
						<div class="col-md-12">
							<label>通知所有metaServer</label>
							<button style="margin-left: 5px" class="btn btn-danger" ng-click="startLeaseAssignings()">通知</button>
							<label style="margin-left: 20px">通知所有<span class="text-danger">fail</span>的metaServer
							</label>
							<button style="margin-left: 5px" class="btn btn-danger" ng-click="startLeaseAssigningsForFaildServers()">通知</button>
							<table class="table table-bordered" style="margin-top: 20px">
								<tr ng-repeat="metaServerRow in metaServers">
									<td ng-repeat="metaServer in metaServerRow track by $index" id="{{metaServer.id}}"><button class="btn" style="padding: 0px 2px;" tooltip="通知" ng-click=startLeaseAssigning(metaServer)>
											<span class="glyphicon glyphicon-bell"></span>
										</button> {{metaServer.id}} <span class="glyphicon glyphicon-repeat" ng-class="{rotate:metaServer.startLeaseAssigning==checkStatus.checking}" ng-show="metaServer.startLeaseAssigning==checkStatus.checking"></span><span class="glyphicon glyphicon-ok-sign" style="color: #5cb85c;" ng-show="metaServer.startLeaseAssigning==checkStatus.success"></span><span class="glyphicon glyphicon-remove-sign" style="color: #d9534f;" ng-show="metaServer.startLeaseAssigning==checkStatus.fail"></span><span
										class="glyphicon glyphicon-question-sign" style="color: #f0ad4e;" ng-show="metaServer.startLeaseAssigning==checkStatus.pedding"></span></td>
								</tr>
							</table>
						</div>
					</div>
					<div class="col-md-3 col-md-offset-1">
						<h4>Step 6:启用租约分配</h4>
						<button class="btn btn-primary nextBtn" type="button" ng-class="{disabled:!stepStatus.step6}">
							Next <span class="glyphicon glyphicon-chevron-right"></span>
						</button>
						<a href="#" class="row skipBtn"><em>Skip this step</em></a> <span class="glyphicon glyphicon-ok-sign" style="margin-left: 10px; color: #5cb85c; font-size: 20px;" ng-show="stepStatus.step6"></span>
						<p class="text-danger stepMessage" style="margin-top: 20px; word-wrap: break-word;"></p>
					</div>
				</div>
				<div class="row setup-content" id="step-7">
					<div class="col-md-8">
						<div class="col-md-12">
							<label>检查所有brokers</label>
							<button style="margin-left: 5px" class="btn btn-danger" ng-click="getRunningBrokers()">检查</button>
							<table class="table table-bordered" style="margin-top: 20px">
								<tr ng-repeat="endpointRow in endpoints">
									<td ng-repeat="endpoint in endpointRow track by $index"><button class="btn" style="padding: 0px 2px;" tooltip="通知" ng-click=resume(metaServer)>
											<span class="glyphicon glyphicon-bell"></span>
										</button> {{endpoint.id}} <span class="glyphicon glyphicon-repeat" ng-class="{rotate:endpoint.isRunning==checkStatus.checking}" ng-show="endpoint.isRunning==checkStatus.checking"></span><span class="glyphicon glyphicon-ok-sign" style="color: #5cb85c;" ng-show="endpoint.isRunning==checkStatus.success"></span><span class="glyphicon glyphicon-remove-sign" style="color: #d9534f;" ng-show="endpoint.isRunning==checkStatus.fail"></span><span class="glyphicon glyphicon-question-sign"
										style="color: #f0ad4e;" ng-show="endpoint.isRunning==checkStatus.pedding"></span></td>
								</tr>
							</table>
						</div>
					</div>
					<div class="col-md-3 col-md-offset-1">
						<h4>Step 7:完成</h4>
						<p class="text-danger stepMessage" style="margin-top: 20px; word-wrap: break-word;"></p>
					</div>
				</div>
			</form>
		</div>
	</div>

	<script type="text/javascript" src="${model.webapp}/js/zk/zk.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/angular/xeditable.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/angular/smart-table.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/bootstrap-multistep.js"></script>
</a:layout>