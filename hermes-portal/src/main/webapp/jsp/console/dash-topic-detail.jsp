<%@ page contentType="text/html; charset=utf-8" isELIgnored="false" trimDirectiveWhitespaces="true"%>
<%@ taglib prefix="a" uri="/WEB-INF/app.tld"%>
<jsp:useBean id="ctx" type="com.ctrip.hermes.portal.console.dashboard.Context" scope="request" />
<jsp:useBean id="payload" type="com.ctrip.hermes.portal.console.dashboard.Payload" scope="request" />
<jsp:useBean id="model" type="com.ctrip.hermes.portal.console.dashboard.Model" scope="request" />

<div class="row">
	<div class="col-md-6">
		<div class="panel panel-success">
			<div class="panel-heading">
				<span class="label label-danger">生产</span>
				<button type="button" data-toggle="modal" data-target="#top-producer-modal" class="btn btn-xs btn-success" style="text-align: center;"><span class="glyphicon glyphicon-th-list"></span>
					生产速度</button>
				<button type="button" data-toggle="modal" data-target="#top-consumer-modal" class="btn btn-xs btn-warning" style="text-align: center;"><span class="glyphicon glyphicon-th-list"></span>
					消费速度</button>
			</div>
			<div class="panel-body">
				<iframe style="border: 0" ng-src="{{get_produced_kibana('${model.kibanaUrl}')}}" height="200" width="100%"></iframe>
			</div>
		</div>
	</div>
	<div class="col-md-6">
		<div class="panel panel-info">
			<div class="panel-heading">Consumer 消费延时</div>
			<table class="table table-hover" st-pipe="topic_delays" st-table="delay_table">
				<thead>
					<tr>
						<th st-sort="consumer">Consumer 名称</th>
						<th st-sort="partition">Partition ID</th>
						<th st-sort="delay">Delay(秒)</th>
					</tr>
					<tr>
						<th><input st-search="consumer" placeholder="Consumer Name" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
						<th><input st-search="partition" placeholder="Partition ID" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
						<th><input st-search="delay" placeholder="Delay" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
					</tr>
				</thead>
				<tbody ng-if="!is_loading">
					<tr ng-click="" ng-repeat="delay in topic_delays">
						<td><span ng-bind="delay.consumer"></span></td>
						<td><span ng-bind="delay.partitionId"></span></td>
						<td><span ng-bind="normalize_delay(delay.delay)"></span></td>
					</tr>
				</tbody>
				<tbody ng-if="is_loading">
					<tr>
						<td colspan="1" class="text-center">Loading ...</td>
					</tr>
				</tbody>
			</table>
		</div>
	</div>
</div>
<div class="row">
	<div class="col-md-6" ng-repeat="delay in topic_delays">
		<div class="panel panel-success">
			<div class="panel-heading">
				<span class="label label-danger">
					消费组：
					<span ng-bind="delay.consumer"></span>
				</span>
			</div>
			<div class="panel-body">
				<iframe style="border: 0" ng-src="{{get_consumed_kibana('${model.kibanaUrl}', delay.consumer)}}" height="200" width="100%"></iframe>
			</div>
		</div>
	</div>
</div>
<div class="modal fade" id="top-producer-modal" tabindex="-1" role="dialog" aria-labelledby="top-producer-label" aria-hidden="true">
	<div class="modal-dialog" style="width: 800px">
		<div class="modal-content">
			<div class="modal-header">
				<button type="button" class="close" data-dismiss="modal" aria-label="Close"><span aria-hidden="true">&times;</span></button>
				<h4 class="modal-title" id="top-producer-label">生产速度/分钟</h4>
			</div>
			<div class="modal-body">
				<div class="row">
					<div class="col-sm-6">
						<div class="panel panel-success">
							<div class="panel-heading">
								<span class="label label-danger">最快排行</span>
							</div>
							<div class="panel-body" style="height: 300px">
								<iframe ng-src="{{get_top_producer_kibana('${model.kibanaUrl}')}}" style="border: 0" width="100%" height="100%"></iframe>
							</div>
						</div>
					</div>
					<div class="col-sm-6">
						<div class="panel panel-success">
							<div class="panel-heading">
								<span class="label label-danger">最慢排行</span>
							</div>
							<div class="panel-body" style="height: 300px">
								<iframe ng-src="{{get_bottom_producer_kibana('${model.kibanaUrl}')}}" style="border: 0" width="100%" height="100%"></iframe>
							</div>
						</div>
					</div>
				</div>
			</div>
		</div>
	</div>
</div>

<div class="modal fade" id="top-consumer-modal" tabindex="-1" role="dialog" aria-labelledby="top-consumer-label" aria-hidden="true">
	<div class="modal-dialog" style="width: 800px">
		<div class="modal-content">
			<div class="modal-header">
				<button type="button" class="close" data-dismiss="modal" aria-label="Close"><span aria-hidden="true">&times;</span></button>
				<h4 class="modal-title" id="top-consumer-label">消费速度</h4>
			</div>
			<div class="modal-body">
				<div class="row">
					<div class="col-sm-6">
						<div class="panel panel-success">
							<div class="panel-heading">
								<span class="label label-danger">最快排行</span>
							</div>
							<div class="panel-body" style="height: 300px">
								<iframe ng-src="{{get_top_consumer_kibana('${model.kibanaUrl}')}}" style="border: 0" width="100%" height="100%"></iframe>
							</div>
						</div>
					</div>
					<div class="col-sm-6">
						<div class="panel panel-success">
							<div class="panel-heading">
								<span class="label label-danger">最慢排行</span>
							</div>
							<div class="panel-body" style="height: 300px">
								<iframe ng-src="{{get_bottom_consumer_kibana('${model.kibanaUrl}')}}" style="border: 0" width="100%" height="100%"></iframe>
							</div>
						</div>
					</div>
				</div>
			</div>
		</div>
	</div>
</div>