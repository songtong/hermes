<%@ page contentType="text/html; charset=utf-8" isELIgnored="false" trimDirectiveWhitespaces="true"%>
<%@ taglib prefix="a" uri="/WEB-INF/app.tld"%>
<jsp:useBean id="ctx" type="com.ctrip.hermes.portal.console.dashboard.Context" scope="request" />
<jsp:useBean id="payload" type="com.ctrip.hermes.portal.console.dashboard.Payload" scope="request" />
<jsp:useBean id="model" type="com.ctrip.hermes.portal.console.dashboard.Model" scope="request" />

<script type="text/javascript">
	var current_client = "${model.clientIP}";
</script>

<a:layout>
	<link href="${model.webapp}/css/dashboard.css" type="text/css" rel="stylesheet">
	<div class="container fluid" ng-app="dash-client" ng-controller="dash-client-controller">
		<div class="row">
			<form action="/console/dashboard">
				<div class="form-group form-inline col-md-12 col-md-offset-4">
					<label>输入客户端IP&emsp;</label>
					<input name="op" value="client" style="display: none;">
					<input name="client" autocomplete="off" ng-model="selected" typeahead="client for client in getClients($viewValue)" typeahead-loading="loadingClients" type="text" class="form-control"
						style="width: 200px" />
					<i ng-show="loadingClients" class="glyphicon glyphicon-refresh"></i> &emsp;
					<button type="submit" class="btn btn-success"><span class="glyphicon glyphicon-arrow-up"></span> 提交</button>
				</div>
			</form>
		</div>
		<div class="row">
			<div class="col-md-12" ng-if="current_ip.length > 0 && (declare_topics.produceTopics.length > 0 || declare_topics.consumeTopics.length > 0)">
				<div class="row">
					<div class="col-md-6" ng-repeat="produced_topic in declare_topics.produceTopics">
						<div class="panel panel-success">
							<div class="panel-heading">
								<span class="label label-primary"></span>
								Topic:
								<span ng-bind="produced_topic"></span>
							</div>
							<div class="panel-body">
								<iframe style="border: 0" ng-src="{{get_client_produced_kibana('${model.kibanaUrl}',produced_topic)}}" height="200px" width="100%"></iframe>
							</div>
						</div>
					</div>
				</div>
				<div class="row">
					<div ng-repeat="group_consumed_topics in declare_topics.consumeTopics">
						<div class="col-md-6" ng-repeat="consumed_topic in group_consumed_topics.value">
							<div class="panel panel-success">
								<div class="panel-heading">
									<span class="label label-primary"></span>
									消费组:
									<span ng-bind="group_consumed_topics.key"></span>
									&emsp;Topic:
									<span ng-bind="consumed_topic"></span>
								</div>
								<div class="panel-body">
									<iframe style="border: 0" ng-src="{{get_client_consumed_kibana('${model.kibanaUrl}',group_consumed_topics.key,consumed_topic)}}" height="200px" width="100%"></iframe>
								</div>
							</div>
						</div>
					</div>
				</div>
			</div>
		</div>
	</div>
	<script type="text/javascript" src="${model.webapp}/js/angular.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/angular-resource.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/smart-table.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/ui-bootstrap-tpls-0.13.0.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/dashboard/client-controller.js"></script>
</a:layout>