<%@ page contentType="text/html; charset=utf-8" isELIgnored="false" trimDirectiveWhitespaces="true"%>
<%@ taglib prefix="a" uri="/WEB-INF/app.tld"%>
<jsp:useBean id="ctx" type="com.ctrip.hermes.portal.console.dashboard.Context" scope="request" />
<jsp:useBean id="payload" type="com.ctrip.hermes.portal.console.dashboard.Payload" scope="request" />
<jsp:useBean id="model" type="com.ctrip.hermes.portal.console.dashboard.Model" scope="request" />

<script>
	var global_kibana_url = "${model.kibanaUrl}"
</script>
<a:layout>
	<link href="${model.webapp}/css/dashboard.css" type="text/css" rel="stylesheet">
	<link href="${model.webapp}/css/bootstrap-treeview.min.css" type="text/css" rel="stylesheet">
	<div class="container fluid" ng-app="dash-topic">
		<div class="row" ng-controller="dash-topic-nav-controller">
			<div class="well col-md-3 sidebar">
				<input class="form-control" ng-model="filter_key">
				<hr>
				<ul class="nav nav-sidebar">
					<li ng-repeat="topic_brief in filtered_topic_briefs" ng-class="'{{topic_brief.topic}}' == nav_current_topic ? 'active' : ''">
						<a href="#detail/{{topic_brief.topic}}" role="tab">
							<span ng-bind="topic_brief.topic | short" tooltip="{{topic_brief.topic}}" tooltip-class="side-tooltip"></span>
							<span style="float: right; margin-top: 6px" class="status-ok" ng-if="topic_brief.dangerLevel==0"></span>
							<span style="float: right; margin-top: 6px" class="status-danger" ng-if="topic_brief.dangerLevel==2"></span>
							<span style="float: right; margin-top: 6px" class="status-warn" ng-if="topic_brief.dangerLevel==1"></span>
						</a>
					</li>
				</ul>
			</div>
			<div ng-view class="col-md-9 col-md-offset-3 main" style="margin-left: 25%"></div>
		</div>
	</div>
	<script type="text/javascript" src="${model.webapp}/js/bootstrap-treeview.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/angular/smart-table.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/angular/angular-route.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/dashboard/dash-topic.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/dashboard/dash-kibana.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/dashboard/dash-topic-controller.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/dashboard/dash-topic-nav-controller.js"></script>
</a:layout>