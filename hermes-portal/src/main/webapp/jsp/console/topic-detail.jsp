<%@ page contentType="text/html; charset=utf-8" isELIgnored="false" trimDirectiveWhitespaces="true"%>
<%@ taglib prefix="a" uri="/WEB-INF/app.tld"%>


<jsp:useBean id="ctx" type="com.ctrip.hermes.portal.console.topic.Context" scope="request" />
<jsp:useBean id="payload" type="com.ctrip.hermes.portal.console.topic.Payload" scope="request" />
<jsp:useBean id="model" type="com.ctrip.hermes.portal.console.topic.Model" scope="request" />

<a:layout>
	<script type="text/javascript">
		var topic_name = "${model.topicName}";
	</script>
	<div class="op-alert alert alert-info" role="alert" style="display: none;">
		<span>The examples populate this alert with dummy content</span>
	</div>

	<div class="panel panel-info">
		<div class="panel-heading">
			<span class="label label-primary">${model.topicName}</span>

			<button class="btn btn-xs btn-success" style="float: right;">
				<span class="glyphicon glyphicon-pencil" aria-hidden="true"></span> 修改
			</button>
		</div>
		<div class="panel-body">
			<div ng-app="hermes-topic-detail" ng-controller="topic-detail-controller">
				<h4>
					<span class="label label-primary">基本信息</span>
				</h4>
				<hr>
				<div class="container row">
					<div class="col-sm-6">
						<form class="form-horizontal">
							<div class="form-group">
								<label class="control-label col-sm-4 detail-label">编码类型</label>
								<select disabled style="width: 150px" class="form-control col-sm-6" ng-model="topic.codecType" ng-options="codec for codec in codec_types">
								</select>
							</div>
							<div class="form-group">
								<label class="control-label col-sm-4 detail-label">存储类型</label>
								<select disabled style="width: 150px" class="form-control col-sm-6" ng-model="topic.storageType" ng-options="storage for storage in storage_types" ng-change="storage_type_changed()">
								</select>
							</div>
							<div class="form-group">
								<label class="control-label col-sm-4 detail-label">Endpoint 类型</label>
								<select disabled style="width: 150px" class="form-control col-sm-6" ng-model="topic.endpointType" ng-options="endpoint for endpoint in endpoint_types" ng-change="endpoint_type_changed()">
								</select>
							</div>
							<div class="form-group">
								<label class="control-label col-sm-4 detail-label">ACK 超时</label>
								<input disabled style="width: 150px" class="form-control col-sm-6" ng-model="topic.ackTimeoutSeconds">
							</div>
							<div class="form-group">
								<label class="control-label col-sm-4 detail-label">消费重试策略</label>
								<input disabled style="width: 150px" class="form-control col-sm-6" ng-model="topic.consumerRetryPolicy">
							</div>
						</form>
					</div>
					<div class="col-sm-6">
						<form class="form-horizontal">
							<div class="form-group">
								<label class="control-label col-sm-3 detail-label">创建者</label>
								<input disabled style="width: 200px" class="form-control col-sm-6" ng-model="topic.createBy">
							</div>
							<div class="form-group">
								<label class="control-label col-sm-3 detail-label">创建时间</label>
								<input disabled style="width: 200px" class="form-control col-sm-6" ng-model="topic.createTime">
							</div>
							<div class="form-group">
								<label class="control-label col-sm-3 detail-label">最后修改时间</label>
								<input disabled style="width: 200px" class="form-control col-sm-6" ng-model="topic.lastModifiedTime">
							</div>
							<div class="form-group">
								<label class="control-label col-sm-3 detail-label">状态</label>
								<input disabled style="width: 150px" class="form-control col-sm-6" ng-model="topic.status">
							</div>
							<div class="form-group">
								<label class="control-label col-sm-3 detail-label">描述</label>
								<input disabled style="width: 150px" class="form-control col-sm-6" ng-model="topic.description">
							</div>
						</form>
					</div>
				</div>
				<h4>
					<span class="label label-primary">Partitions</span>
				</h4>
				<hr>
				<table class="table table-condensed table-responsive">
					<thead>
						<tr>
							<th style="border: none; width: 5%">#</th>
							<th style="border: none;">Read DS</th>
							<th style="border: none;">Write DS</th>
							<th style="border: none;">Endpoint</th>
						</tr>
					</thead>
					<tbody>
						<tr ng-repeat="partition in topic.partitions">
							<td style="border: none; width: 5%;"><label class="detail-label" ng-bind="$index + 1"></label></td>
							<td style="border: none;"><select disabled class="form-control" ng-model="partition.readDatasource" ng-options="ds for ds in datasource_names"></select></td>
							<td style="border: none;"><select disabled class="form-control" ng-model="partition.writeDatasource" ng-options="ds for ds in datasource_names"></select></td>
							<td style="border: none;"><select disabled class="form-control" ng-model="partition.endpoint" ng-options="ep for ep in endpoint_names"></select></td>
						</tr>
					</tbody>
				</table>
				<h4>
					<span class="label label-primary">Consumers</span>
				</h4>
				<hr>
			</div>
		</div>
	</div>

	<script type="text/javascript" src="${model.webapp}/js/angular.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/angular-resource.min.js"></script>

	<script type="text/javascript" src="${model.webapp}/js/topic-detail.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/bootbox.min.js"></script>
</a:layout>