<%@ page contentType="text/html; charset=utf-8" isELIgnored="false" trimDirectiveWhitespaces="true"%>
<%@ taglib prefix="a" uri="/WEB-INF/app.tld"%>
<jsp:useBean id="ctx" type="com.ctrip.hermes.portal.console.dashboard.Context" scope="request" />
<jsp:useBean id="payload" type="com.ctrip.hermes.portal.console.dashboard.Payload" scope="request" />
<jsp:useBean id="model" type="com.ctrip.hermes.portal.console.dashboard.Model" scope="request" />

<a:layout>
	<link href="${model.webapp}/css/dashboard.css" type="text/css" rel="stylesheet">
	<div class="container fluid" ng-app="dash-topic" ng-controller="dash-topic-controller">
		<div class="row">
			<div class="col-sm-3 col-md-2 sidebar">
				<ul class="nav nav-sidebar">
					<li ng-click="nav_select(topic_brief)" ng-repeat="topic_brief in topic_briefs" role="presentation" ng-class="$first ? 'active' : ''"><a role="tab" data-toggle="tab" aria-controls="content">
							<span ng-bind="topic_brief.topic" style="text-transform: capitalize;"></span> <span style="float: right; margin-top: 6px" class="status-ok" ng-if="topic_brief.dangerLevel==0"></span> <span
								style="float: right; margin-top: 6px" class="status-danger" ng-if="topic_brief.dangerLevel==2"></span> <span style="float: right; margin-top: 6px" class="status-warn"
								ng-if="topic_brief.dangerLevel==1"></span>
					</a></li>
				</ul>
			</div>
			<div id="main_board" class="col-sm-9 col-md-10 main col-md-offset-2"></div>
		</div>
	</div>
	<script type="text/javascript" src="${model.webapp}/js/angular.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/angular-resource.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/angular-route.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/smart-table.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/ui-bootstrap-tpls-0.13.0.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/dashboard/service.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/dashboard/controller.js"></script>

</a:layout>