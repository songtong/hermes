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
	<div class="container fluid" ng-app="dash-monitor-event">
		<div class="row" ng-controller="dash-monitor-event-controller">
			<div class="panel panel-success">
				<div class="panel-heading">Monitor Event</div>
				<table class="table table-hover" st-pipe="find_monitor_events" st-table="monitor_event_displayed" style="font-size: small;">
					<thead>
						<tr>
							<th>事件类型</th>
							<th>创建时间</th>
							<th>标签1</th>
							<th>标签2</th>
							<th>标签3</th>
							<th>标签4</th>
							<th>事件内容</th>
							<th>报警时间</th>
						</tr>
					</thead>
					<tbody ng-if="!is_loading">
						<tr ng-repeat="monitor_event in monitor_event_displayed">
							<td><span ng-bind="monitor_event.eventType"></span></td>
							<td><span ng-bind="monitor_event.createTime | date:'yyyy-MM-dd HH:mm:ss'"></span></td>
							<td><a href="" tooltip="{{monitor_event.key1}}">
									<span ng-bind="monitor_event.key1 | short:25"></span>
								</a></td>
							<td><a href="" tooltip="{{monitor_event.key2}}">
									<span ng-bind="monitor_event.key2 | short:25"></span>
								</a></td>
							<td><a href="" tooltip="{{monitor_event.key3}}">
									<span ng-bind="monitor_event.key3 | short:25"></span>
								</a></td>
							<td><a href="" tooltip="{{monitor_event.key4}}">
									<span ng-bind="monitor_event.key4 | short:25"></span>
								</a></td>
							<td><a href="" tooltip="点击查看详情" ng-click="show_message(monitor_event.message)">
									<span ng-bind="monitor_event.message | short:20"></span>
								</a></td>
							<td><span ng-bind="monitor_event.notifyTime || 'NOT_YET'"></span></td>
						</tr>
					</tbody>
					<tbody ng-if="is_loading">
						<tr>
							<td colspan="1" class="text-center">Loading ...</td>
						</tr>
					</tbody>
					<tfoot>
						<tr>
							<td colspan="8" class="text-center">
								<div st-pagination="" st-items-by-page="15" st-displayed-pages="10"></div>
							</td>
						</tr>
					</tfoot>
				</table>
			</div>
			<div class="modal fade" id="message-view" tabindex="-1" role="dialog" aria-labelledby="message-label" aria-hidden="true">
				<div class="modal-dialog" style="width: 1024px">
					<div class="modal-content">
						<div class="modal-header">
							<button type="button" class="close" data-dismiss="modal" aria-label="Close"><span aria-hidden="true">&times;</span></button>
							<h4 class="modal-title" id="message-label">Message</h4>
						</div>
						<div class="modal-body">
							<div class="container-fluid">
								<pre style="max-height: 600px; overflow: scroll;">{{current_message}}</pre>
							</div>
						</div>
					</div>
				</div>
			</div>
		</div>
	</div>


	<script type="text/javascript" src="${model.webapp}/js/bootstrap-treeview.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/angular/smart-table.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/dashboard/dash-monitor.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/dashboard/dash-monitor-controller.js"></script>
</a:layout>