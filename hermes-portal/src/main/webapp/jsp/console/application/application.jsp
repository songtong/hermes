<%@ page contentType="text/html; charset=utf-8" isELIgnored="false" trimDirectiveWhitespaces="true"%>
<%@ taglib prefix="a" uri="/WEB-INF/app.tld"%>
<jsp:useBean id="ctx" type="com.ctrip.hermes.portal.console.application.Context" scope="request" />
<jsp:useBean id="payload" type="com.ctrip.hermes.portal.console.application.Payload" scope="request" />
<jsp:useBean id="model" type="com.ctrip.hermes.portal.console.application.Model" scope="request" />

<a:layout>
	<link href="${model.webapp}/css/xeditable.css" type="text/css" rel="stylesheet">
	<link href="${model.webapp}/css/font-awesome.min.css" type="text/css" rel="stylesheet">
	<link href="${model.webapp}/css/bootstrap-slider.min.css" type="text/css" rel="stylesheet">
	
	<div class="container fluid" ng-app="application" style="width: 90%;">
		<div ng-view class="main"></div>
	</div>
	<script type="text/javascript" src="${model.webapp}/js/angular/angular-route.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/angular/xeditable.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/angular/smart-table.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/bootstrap-slider.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/slider.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/lib/global.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/lib/utils.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/lib/user.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/lib/topic-sync.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/lib/components.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/application/application.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/application/app-topic-controller.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/application/app-review-controller.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/application/app-approval-list-controller.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/application/app-review-list-controller.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/application/app-approval-detail-controller.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/application/app-consumer-controller.js"></script>
</a:layout>