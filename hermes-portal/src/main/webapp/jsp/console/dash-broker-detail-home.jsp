<%@ page contentType="text/html; charset=utf-8" isELIgnored="false" trimDirectiveWhitespaces="true"%>
<%@ taglib prefix="a" uri="/WEB-INF/app.tld"%>
<jsp:useBean id="ctx" type="com.ctrip.hermes.portal.console.dashboard.Context" scope="request" />
<jsp:useBean id="payload" type="com.ctrip.hermes.portal.console.dashboard.Payload" scope="request" />
<jsp:useBean id="model" type="com.ctrip.hermes.portal.console.dashboard.Model" scope="request" />

<div class="row">
	<div class="row">
		<div class="col-md-6">
			<div class="panel panel-success">
				<div class="panel-heading">
					<span class="label label-primary">Received Top</span>
				</div>
				<div class="panel-body">
					<iframe style="border: 0" ng-src="{{get_received_top_kibana('${model.kibanaUrl}')}}" height="400px" width="100%"></iframe>
				</div>
			</div>
		</div>
		<div class="col-md-6">
			<div class="panel panel-success">
				<div class="panel-heading">
					<span class="label label-primary">Delivered Top</span>
				</div>
				<div class="panel-body">
					<iframe style="border: 0" ng-src="{{get_delivered_top_kibana('${model.kibanaUrl}')}}" height="400px" width="100%"></iframe>
				</div>
			</div>
		</div>
	</div>
</div>