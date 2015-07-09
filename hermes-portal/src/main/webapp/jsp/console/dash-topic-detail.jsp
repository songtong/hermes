<%@ page contentType="text/html; charset=utf-8" isELIgnored="false" trimDirectiveWhitespaces="true"%>
<%@ taglib prefix="a" uri="/WEB-INF/app.tld"%>
<jsp:useBean id="ctx" type="com.ctrip.hermes.portal.console.dashboard.Context" scope="request" />
<jsp:useBean id="payload" type="com.ctrip.hermes.portal.console.dashboard.Payload" scope="request" />
<jsp:useBean id="model" type="com.ctrip.hermes.portal.console.dashboard.Model" scope="request" />

<html>
<body>
	<div class="container fluid">
		<span ng-bind="current_topic"></span>
		<div class="row">
			<div class="col-sm-3 col-md-5">
				<div class="panel panel-info">
					<div class="panel-heading">Consumer 消费延时</div>
					<table class="table table-hover" st-pipe="get_topics" st-table="topic_table">
						<thead>
							<tr>
								<th st-sort="name">Topic名称</th>
							</tr>
							<tr>
								<th><label><span ng-bind="topic_rows.length"></span></label></th>
							</tr>
						</thead>
						<tbody ng-if="!is_loading">
							<tr ng-click="" ng-repeat="row in topic_rows">
								<td><span ng-bind="$index + 1"> </span></td>
							</tr>
						</tbody>
						<tbody ng-if="is_loading">
							<tr>
								<td colspan="1" class="text-center">Loading ...</td>
							</tr>
						</tbody>
					</table>
				</div>
			</div>
		</div>
	</div>
</body>
</html>