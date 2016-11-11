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
					<h2 style="text-align: center;">
						<label class="label label-warning">HELLO HERMES!</label>
					</h2>
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
					<h2 style="text-align: center;">
						<label class="label label-warning">HELLO HERMES!</label>
					</h2>
				</div>
			</div>
			<div class="col-md-6">
				<div class="panel panel-info">
					<div class="panel-heading">Broker投递排行</div>
					<h2 style="text-align: center;">
						<label class="label label-warning">HELLO HERMES!</label>
					</h2>
				</div>
			</div>
		</div>
	</div>

	<script type="text/javascript" src="${model.webapp}/js/angular/smart-table.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/home/home.js"></script>
</a:layout>