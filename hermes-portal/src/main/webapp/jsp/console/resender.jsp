<%@ page contentType="text/html; charset=utf-8" isELIgnored="false" trimDirectiveWhitespaces="true"%>
<%@ taglib prefix="a" uri="/WEB-INF/app.tld"%>
<jsp:useBean id="ctx" type="com.ctrip.hermes.portal.console.resender.Context" scope="request" />
<jsp:useBean id="payload" type="com.ctrip.hermes.portal.console.resender.Payload" scope="request" />
<jsp:useBean id="model" type="com.ctrip.hermes.portal.console.resender.Model" scope="request" />

<a:layout>
	<div class="op-alert alert alert-info" role="alert" style="display: none;">
		<span>The examples populate this alert with dummy content</span>
	</div>
	<div class="container row" ng-app="hermes-resender" ng-controller="resender-controller">
		<br>
		<form class="form-horizontal">
			<div class="form-group">
				<label for="input_topic" class="control-label col-sm-5">Topic</label>
				<div class="col-sm-5">
					<input ng-model="topic" type="text" id="input_topic" class="form-control" placeholder="Topic">
				</div>
			</div>
			<div class="form-group">
				<label for="input_msg" class="control-label col-sm-5">Message</label>
				<div class="col-sm-5">
					<input ng-model="message_content" type="text" id="input_msg" class="form-control" placeholder="Message">
				</div>
			</div>
			<div class="form-group">
				<div class="col-sm-offset-5 col-sm-5">
					<button type="submit" class="btn btn-success" ng-click="send_message()"><span class="glyphicon glyphicon-send"></span> 发送</button>
				</div>
			</div>
		</form>
	</div>
	<script type="text/javascript" src="${model.webapp}/js/angular.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/angular-resource.min.js"></script>
	<script src="${model.webapp}/js/resender.js"></script>
</a:layout>