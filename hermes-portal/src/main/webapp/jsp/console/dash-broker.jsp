<%@ page contentType="text/html; charset=utf-8" isELIgnored="false" trimDirectiveWhitespaces="true"%>
<%@ taglib prefix="a" uri="/WEB-INF/app.tld"%>
<jsp:useBean id="ctx" type="com.ctrip.hermes.portal.console.dashboard.Context" scope="request" />
<jsp:useBean id="payload" type="com.ctrip.hermes.portal.console.dashboard.Payload" scope="request" />
<jsp:useBean id="model" type="com.ctrip.hermes.portal.console.dashboard.Model" scope="request" />

<a:layout>
	<link href="${model.webapp}/css/dashboard.css" type="text/css" rel="stylesheet">
	<div class="container fluid" ng-app="dash-broker" ng-controller="dash-broker-controller">
		<div class="row">
			<div class="col-md-2 sidebar">
				<ul class="nav nav-sidebar">
					<li ng-click="nav_select('_broker_main')" class="active" role="presentation">
						<a href="" role="tab" data-toggle="tab" aria-controls="content"> OVERVIEW </a>
					</li>
					<li ng-click="nav_select(broker)" ng-repeat="broker in brokers" role="presentation">
						<a href="" role="tab" data-toggle="tab" aria-controls="content">
							<span ng-bind="broker" style="text-transform: capitalize;"></span>
						</a>
					</li>
				</ul>
			</div>
			<div id="main_board" class="col-md-10 col-md-offset-2 main" style="margin-left: 15%"></div>
		</div>
	</div>
	<script type="text/javascript" src="${model.webapp}/js/angular.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/angular-resource.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/smart-table.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/ui-bootstrap-tpls-0.13.0.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/dashboard/broker-controller.js"></script>
</a:layout>