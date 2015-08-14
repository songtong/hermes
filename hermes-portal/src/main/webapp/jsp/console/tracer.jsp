<%@ page contentType="text/html; charset=utf-8" isELIgnored="false" trimDirectiveWhitespaces="true"%>
<%@ taglib prefix="a" uri="/WEB-INF/app.tld"%>
<jsp:useBean id="ctx" type="com.ctrip.hermes.portal.console.tracer.Context" scope="request" />
<jsp:useBean id="payload" type="com.ctrip.hermes.portal.console.tracer.Payload" scope="request" />
<jsp:useBean id="model" type="com.ctrip.hermes.portal.console.tracer.Model" scope="request" />

<a:layout>
	<script>
		var esUrl = "${model.esHost}";
	</script>
	<div class="container row" ng-app="hermes-tracer" ng-controller="tracer-controller">
		<div class="form-group form-inline" align="center" style="margin-left: 100px">
			<label>输入 Ref-Key</label>
			<input type="text" class="form-control" style="width: 400px" ng-model="ref_key" />
			<input type="date" class="form-control" ng-model="msg_date" placeholder="yyyy-MM-dd" min="2015-05-01" max="2115-12-31" />
			<button class="btn btn-success" ng-click="show_message(ref_key, msg_date)"><span class="glyphicon glyphicon-arrow-up"></span> 提交</button>
		</div>
	</div>
	<script type="text/javascript" src="${model.webapp}/js/d3/d3.min.js" type="text/JavaScript"></script>
	<script type="text/javascript" src="${model.webapp}/js/highcharts/highcharts.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/highcharts/highcharts-more.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/highcharts/exporting.js"></script>

	<script src="${model.webapp}/js/tracer/tracer.js"></script>
	<div id="container" style="min-width: 310px; height: 400px; margin: 0 auto"></div>
</a:layout>