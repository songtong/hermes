<%@ page contentType="text/html; charset=utf-8" isELIgnored="false" trimDirectiveWhitespaces="true"%>
<%@ taglib prefix="a" uri="/WEB-INF/app.tld"%>
<jsp:useBean id="ctx" type="com.ctrip.hermes.portal.console.home.Context" scope="request" />
<jsp:useBean id="payload" type="com.ctrip.hermes.portal.console.home.Payload" scope="request" />
<jsp:useBean id="model" type="com.ctrip.hermes.portal.console.home.Model" scope="request" />

<a:layout>

	<div id="dashboard-app" class="container fluid" ng-app="dashboard" ng-controller="hermes-dashboard-controller">
		<div class="row" style="height: 300px">
			<div class="col-md-6">
				<div class="panel panel-info">
					<div class="panel-heading">消费延迟排行</div>
					<table class="table table-hover" st-pipe="get_consume_delays" st-safe-src="consume_delays_detail" st-table="display_consume_delays_deail">
						<thead>
							<tr>
								<th st-sort="topic">Topic</th>
								<th st-sort="consumer">Consumer</th>
								<th st-sort="delay">Delay(条)</th>
							</tr>
							<tr>
								<th><input st-search="topic" placeholder="Topic" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
								<th><input st-search="consumer" placeholder="Consumer" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
							</tr>
						</thead>
						<tbody ng-if="!delay_table_is_loading">
							<tr ng-repeat="delay in display_consume_delays_deail">
								<td><a href="/console/dashboard#/detail/{{delay.topic}}" tooltip="{{delay.topic}}"> <span ng-bind="delay.topic | short:25"></span>
								</a></td>
								<td><a href="/console/dashboard#/consume/{{delay.topic}}/{{delay.consumer}}" tooltip="{{delay.consumer}}"> <span ng-bind="delay.consumer | short:25"></span>
								</a></td>
								<td><a href="" popover-placement="right" popover-trigger="focus" popover-template="get_delay_detail(delay)" popover-title="{{delay.topic}}"> <span ng-bind="delay.delay"></span>
								</a></td>
							</tr>
						</tbody>
						<tbody ng-if="delay_table_is_loading">
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
					<table class="table table-hover" st-pipe="get_outdate_topics" st-safe-src="outdate_topics" st-table="display_outdate_topics">
						<thead>
							<tr>
								<th st-sort="key">Topic</th>
								<th st-sort="value">最近生产</th>
								<th st-sort="getters.outdate_delay_to_now">延时</th>
							</tr>
							<tr>
								<th><input st-search="key" placeholder="Topic" class="input-sm form-control" type="search" ng-model-options="{updateOn:'blur'}" /></th>
							</tr>
						</thead>
						<tbody ng-if="!outdate_topics_table_is_loading">
							<tr ng-repeat="outdate in display_outdate_topics">
								<td><a href="/console/dashboard#/latest/{{outdate.key}}" tooltip="{{outdate.key}}"> <span ng-bind="outdate.key | short:20"></span>
								</a></td>
								<td><span ng-bind="outdate.value | date:'yyyy-MM-dd HH:mm:ss' | produce_format"></span></td>
								<td><span ng-bind="get_delay_to_now(outdate.value)"></span></td>
							</tr>
						</tbody>
						<tbody ng-if="outdate_topics_table_is_loading">
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
					<table class="table table-hover" st-pipe="broker_received_qps" st-safe-src="broker_received_qps" st-table="display_broker_received_qps">
						<thead>
							<tr>
								<th st-sort="broker">Broker</th>
								<th st-sort="qps">Speed(分钟)</th>
							</tr>
						</thead>
						<tbody ng-if="!broker_received_table_is_loading">
							<tr ng-repeat="qps in display_broker_received_qps">
								<td><span ng-bind="qps.brokerIp"></span></td>
								<td><a href="" ng-click="get_broker_received_detail(qps.brokerIp)" data-toggle="modal" data-target="#broker_received_modal"> <span ng-bind="qps.qps"></span>
								</a></td>
							</tr>
						</tbody>
						<tbody ng-if="broker_received_table_is_loading">
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
					<table class="table table-hover" st-pipe="broker_delivered_qps" st-safe-src="broker_delivered_qps" st-table="display_broker_delivered_qps">
						<thead>
							<tr>
								<th st-sort="broker">Broker</th>
								<th st-sort="delay">Speed(分钟)</th>
							</tr>
						</thead>
						<tbody ng-if="!broker_delivered_table_is_loading">
							<tr ng-repeat="qps in display_broker_delivered_qps">
								<td><span ng-bind="qps.brokerIp"></span></td>
								<td><a href="" ng-click="get_broker_delivered_detail(qps.brokerIp)" data-toggle="modal" data-target="#broker_delivered_modal"> <span ng-bind="qps.qps"></span>
								</a></td>
							</tr>
						</tbody>
						<tbody ng-if="broker_delivered_table_is_loading">
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
						<button type="button" class="close" data-dismiss="modal" aria-label="Close">
							<span aria-hidden="true">&times;</span>
						</button>
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
						<button type="button" class="close" data-dismiss="modal" aria-label="Close">
							<span aria-hidden="true">&times;</span>
						</button>
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


	<script type="text/javascript" src="${model.webapp}/js/angular/smart-table.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/home/home.js"></script>
</a:layout>