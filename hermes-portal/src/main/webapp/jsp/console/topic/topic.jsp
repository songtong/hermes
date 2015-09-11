<%@ page contentType="text/html; charset=utf-8" isELIgnored="false" trimDirectiveWhitespaces="true"%>
<%@ taglib prefix="a" uri="/WEB-INF/app.tld"%>


<jsp:useBean id="ctx" type="com.ctrip.hermes.portal.console.topic.Context" scope="request" />
<jsp:useBean id="payload" type="com.ctrip.hermes.portal.console.topic.Payload" scope="request" />
<jsp:useBean id="model" type="com.ctrip.hermes.portal.console.topic.Model" scope="request" />

<a:layout>
	<link href="${model.webapp}/css/bootstrap-treeview.min.css" type="text/css" rel="stylesheet">
	<link href="${model.webapp}/css/xeditable.css" type="text/css" rel="stylesheet">
	<div class="container fluid" ng-app="topic" ng-init="delayLimit=300;noProduceLimit=432000000;current_topic_type='mysql'">
		<div class="row">
			<div class="col-md-2 sidebar" ng-controller="list-controller">
				<ul class="nav nav-sidebar">
					<li ng-class="routeParams['type'] == 'mysql' ? 'active' : ''">
						<a href="#list/mysql" data-target="#mysql" role="tab" data-toggle="tab" style="text-transform: uppercase;">mysql</a>
					</li>
					<li ng-class="routeParams['type'] == 'kafka' ? 'active' : ''">
						<a href="#list/kafka" data-target="#kafka" role="tab" data-toggle="tab" style="text-transform: uppercase;">kafka</a>
					</li>
				</ul>
			</div>
			<div ng-view class="col-md-10 col-md-offset-2 main"></div>
		</div>
	</div>
	<script type="text/javascript" src="${model.webapp}/js/bootstrap-treeview.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/angular/angular-route.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/angular/angular-upload.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/angular/smart-table.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/angular/xeditable.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/topic/topic.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/topic/topic-list-controller.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/topic/mysql-add-controller.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/topic/kafka-add-controller.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/topic/mysql-detail-controller.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/topic/kafka-detail-controller.js"></script>
</a:layout>