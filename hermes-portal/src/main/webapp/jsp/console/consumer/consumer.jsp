<%@ page contentType="text/html; charset=utf-8" isELIgnored="false" trimDirectiveWhitespaces="true"%>
<%@ taglib prefix="a" uri="/WEB-INF/app.tld"%>

<jsp:useBean id="ctx" type="com.ctrip.hermes.portal.console.consumer.Context" scope="request" />
<jsp:useBean id="payload" type="com.ctrip.hermes.portal.console.consumer.Payload" scope="request" />
<jsp:useBean id="model" type="com.ctrip.hermes.portal.console.consumer.Model" scope="request" />

<a:layout>
	<link href="${model.webapp}/css/xeditable.css" type="text/css" rel="stylesheet">
	<link href="${model.webapp}/css/font-awesome.min.css" type="text/css" rel="stylesheet">
	<div class="container fluid" ng-app="consumer_module" style="width: 100%;">
		<div ng-view class="main"></div>
	</div>

	<script type="text/javascript" src="${model.webapp}/js/angular/angular-route.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/angular/xeditable.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/angular/smart-table.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/lib/date.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/lib/global.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/lib/utils.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/lib/components.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/consumer/consumer.js"></script>
</a:layout>