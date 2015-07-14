<%@ page contentType="text/html; charset=utf-8" isELIgnored="false" trimDirectiveWhitespaces="true"%>
<%@ taglib prefix="a" uri="/WEB-INF/app.tld"%>
<jsp:useBean id="ctx" type="com.ctrip.hermes.portal.console.home.Context" scope="request" />
<jsp:useBean id="payload" type="com.ctrip.hermes.portal.console.home.Payload" scope="request" />
<jsp:useBean id="model" type="com.ctrip.hermes.portal.console.home.Model" scope="request" />

<a:layout>

	<div class="container fluid" ng-app="dashboard" ng-controller="hermes-dashboard-controller">
		<div class="row" style="height: 300px">
			<div class="col-md-6">
				<div class="panel panel-info">
					<div class="panel-heading">消费延迟排行</div>
					<table class="table table-hover" st-pipe="consume_delays" st-table="delay_table">
						<thead>
							<tr>
								<th st-sort="topic">Topic</th>
								<th st-sort="delay">Delay(秒)</th>
							</tr>
						</thead>
						<tbody ng-if="!is_loading">
							<tr ng-repeat="delay in consume_delays">
								<td><span ng-bind="delay.topic"></span></td>
								<td><span popover-placement="bottom" popover-trigger="mouseenter" popover-template="get_delay_detail(delay.topic)" popover-title="{{delay.topic}}"
										ng-bind="normalize_delay(delay.averageDelay)"></span></td>
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
			<div class="col-md-6">
				<div class="panel panel-info">
					<div class="panel-heading">过期Topic排行</div>
					<table class="table table-hover" st-pipe="outdate_topics" st-table="outdate_topics_table">
						<thead>
							<tr>
								<th st-sort="topic">Topic</th>
								<th st-sort="latest">最近生产</th>
								<th st-sort="delay">延时</th>
							</tr>
						</thead>
						<tbody ng-if="!is_loading">
							<tr ng-repeat="outdate in outdate_topics">
								<td><span ng-bind="outdate.key"></span></td>
								<td><span ng-bind="outdate.value | date:'yyyy-MM-dd HH:mm:ss'"></span></td>
								<td><span ng-bind="get_delay_to_now(outdate.value)"></span></td>
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
		<div class="row" style="height: 300px">
			<div class="col-md-6">
				<div class="panel panel-info">
					<div class="panel-heading">Broker接收排行</div>
					<table class="table table-hover" st-pipe="broker_received_qps" st-table="broker_received_table">
						<thead>
							<tr>
								<th st-sort="broker">Broker</th>
								<th st-sort="qps">Speed(分钟)</th>
							</tr>
						</thead>
						<tbody ng-if="!is_loading">
							<tr ng-repeat="qps in broker_received_qps">
								<td><span ng-bind="qps.brokerIp"></span></td>
								<td><a href="" ng-click="get_broker_received_detail(qps.brokerIp)" data-toggle="modal" data-target="#broker_received_modal">
										<span ng-bind="qps.qps"></span>
									</a></td>
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
			<div class="col-md-6">
				<div class="panel panel-info">
					<div class="panel-heading">Broker投递排行</div>
					<table class="table table-hover" st-pipe="broker_delivered_qps" st-table="broker_delivered_table">
						<thead>
							<tr>
								<th st-sort="broker">Broker</th>
								<th st-sort="delay">Speed(分钟)</th>
							</tr>
						</thead>
						<tbody ng-if="!is_loading">
							<tr ng-repeat="qps in broker_delivered_qps">
								<td><span ng-bind="qps.brokerIp"></span></td>
								<td><a href="" ng-click="get_broker_delivered_detail(qps.brokerIp)" data-toggle="modal" data-target="#broker_delivered_modal">
										<span ng-bind="qps.qps"></span>
									</a></td>
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
		<div class="modal fade" id="broker_received_modal" tabindex="-1" role="dialog" aria-labelledby="broker_received_label" aria-hidden="true">
			<div class="modal-dialog">
				<div class="modal-content">
					<div class="modal-header">
						<button type="button" class="close" data-dismiss="modal" aria-label="Close"><span aria-hidden="true">&times;</span></button>
						<h4 class="modal-title" id="broker_received_label">
							<span ng-bind="current_broker_received_details.brokerIp"></span>
						</h4>
					</div>
					<div class="modal-body">
						<table class="table table-striped table-bordered">
							<thead>
								<tr>
									<th>Topic</th>
									<th>Speed(分钟)</th>
								</tr>
							</thead>
							<tbody>
								<tr ng-repeat="detail in current_broker_received_details.qpsDetail">
									<td><span ng-bind="detail.topic"></span></td>
									<td><span ng-bind="detail.qps"></span></td>
								</tr>
							</tbody>
						</table>
					</div>
				</div>
			</div>
		</div>
		<div class="modal fade" id="broker_delivered_modal" tabindex="-1" role="dialog" aria-labelledby="broker_delivered_label" aria-hidden="true">
			<div class="modal-dialog">
				<div class="modal-content">
					<div class="modal-header">
						<button type="button" class="close" data-dismiss="modal" aria-label="Close"><span aria-hidden="true">&times;</span></button>
						<h4 class="modal-title" id="broker_delivered_label">
							<span ng-bind="current_broker_delivered_details.brokerIp"></span>
						</h4>
					</div>
					<div class="modal-body">
						<table class="table table-striped table-bordered">
							<thead>
								<tr>
									<th>Topic</th>
									<th>Speed(分钟)</th>
								</tr>
							</thead>
							<tbody>
								<tr ng-repeat="detail in current_broker_delivered_details.qpsDetail">
									<td><span ng-bind="detail.topic"></span></td>
									<td><span ng-bind="detail.qps"></span></td>
								</tr>
							</tbody>
						</table>
					</div>
				</div>
			</div>
		</div>
	</div>
	<script type="text/ng-template" id="delay_popover_template">
		<table class="table table-condensed table-striped table-bordered">
			<thead>
				<tr>
					<th>Consumer</th>
					<th>Partition</th>
					<th>Delay</th>
				</tr>
			</thead>
			<tbody>
				<tr ng-repeat="detail in current_delay_details">
					<td><span ng-bind="detail.consumer"></span></td>
					<td><span ng-bind="detail.partitionId"></span></td>
					<td><span ng-bind="normalize_delay(detail.delay)"></span></td>
				</tr>
			</tbody>
		</table>
	</script>

	<script type="text/javascript" src="${model.webapp}/js/angular.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/angular-resource.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/smart-table.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/ui-bootstrap-tpls-0.13.0.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/home/home.js"></script>
</a:layout>