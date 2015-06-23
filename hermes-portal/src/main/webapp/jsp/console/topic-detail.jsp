<%@ page contentType="text/html; charset=utf-8" isELIgnored="false" trimDirectiveWhitespaces="true"%>
<%@ taglib prefix="a" uri="/WEB-INF/app.tld"%>


<jsp:useBean id="ctx" type="com.ctrip.hermes.portal.console.topic.Context" scope="request" />
<jsp:useBean id="payload" type="com.ctrip.hermes.portal.console.topic.Payload" scope="request" />
<jsp:useBean id="model" type="com.ctrip.hermes.portal.console.topic.Model" scope="request" />

<a:layout>
	<link href="${model.webapp}/css/xeditable.css" type="text/css" rel="stylesheet">
	<script type="text/javascript">
		var topic_name = "${model.topicName}";
	</script>
	<div class="panel panel-info" ng-app="hermes-topic-detail" ng-controller="topic-detail-controller">
		<div class="panel-heading">
			<span class="label label-primary">${model.topicName}</span>
			<button class="btn btn-xs btn-success" style="float: right;" aria-hidden="true" ng-click="basic_form.$show()"><span class="glyphicon glyphicon-pencil"></span> 修改</button>
		</div>
		<div class="panel-body">
			<h4>
				<span class="label label-primary">基本信息</span>
			</h4>
			<hr>
			<form editable-form name="basic_form">
				<div class="container row">
					<div class="col-sm-6">
						<div class="form-horizontal">
							<div class="form-group">
								<label class="control-label col-sm-4 detail-label">编码类型</label><span ng-bind='topic.codecType' editable-select="topic.codecType" style="line-height: 3;"
									e-ng-options="codec for codec in codec_types"></span>
							</div>
							<div class="form-group">
								<label class="control-label col-sm-4 detail-label">存储类型</label> <span ng-bind='topic.storageType' style="line-height: 3"></span>
							</div>
							<div class="form-group">
								<label class="control-label col-sm-4 detail-label">Endpoint 类型</label> <span ng-bind='topic.endpointType' style="line-height: 3"></span>
							</div>
							<div class="form-group">
								<label class="control-label col-sm-4 detail-label">ACK 超时</label><span ng-bind='topic.ackTimeoutSeconds' editable-range="topic.ackTimeoutSeconds" style="line-height: 3" e-step="5" e-min="5"
									e-max="200"></span>
							</div>
							<div class="form-group">
								<label class="control-label col-sm-4 detail-label">消费重试策略</label><span ng-bind='topic.consumerRetryPolicy' editable-text="topic.consumerRetryPolicy" style="line-height: 3"></span>
							</div>
						</div>
					</div>
					<div class="col-sm-6">
						<div class="form-horizontal">
							<div class="form-group">
								<label class="control-label col-sm-3 detail-label">创建者</label><span ng-bind='topic.createBy || "None"' editable-text="topic.createBy" style="line-height: 3"></span>
							</div>
							<div class="form-group">
								<label class="control-label col-sm-3 detail-label">创建时间</label> <span style="line-height: 3" ng-bind="topic.createTime"></span>
							</div>
							<div class="form-group">
								<label class="control-label col-sm-3 detail-label">最后修改时间</label> <span style="line-height: 3" ng-bind="topic.lastModifiedTime"></span>
							</div>
							<div class="form-group">
								<label class="control-label col-sm-3 detail-label">状态</label><span style="line-height: 3" ng-bind="topic.status"></span>
							</div>
							<div class="form-group">
								<label class="control-label col-sm-3 detail-label">描述</label><span ng-bind='topic.description || "None"' editable-text="topic.description" style="line-height: 3"></span>
							</div>
						</div>
					</div>
				</div>
			</form>
			<h4>
				<span class="label label-primary">Partitions</span>
			</h4>
			<hr>
			<table class="table table-condensed table-responsive table-border">
				<thead>
					<tr>
						<th style="width: 5%">#</th>
						<th>Read DS</th>
						<th>Write DS</th>
						<th>Endpoint</th>
						<th width="100px"><button class="btn btn-success btn-xs" ng-click="add_partition()"><span class="glyphicon glyphicon-plus"></span> 新增</button></th>
					</tr>
				</thead>
				<tbody>
					<tr ng-repeat="partition in topic.partitions">
						<td style="width: 5%;"><label ng-bind="$index + 1"></label></td>
						<td><span e-form="rowform" e-name="readDatasource" ng-bind="partition.readDatasource" editable-select="partition.readDatasource" e-ng-options="storage for storage in datasource_names"></span></td>
						<td><span e-form="rowform" e-name="writeDatasource" ng-bind="partition.writeDatasource" editable-select="partition.writeDatasource" e-ng-options="storage for storage in datasource_names"></span></td>
						<td><span e-form="rowform" e-name="endpoint" ng-bind="partition.endpoint" editable-select="partition.endpoint" e-ng-options="endpoint for endpoint in endpoint_names"></span></td>
						<td width="150px">
							<form editable-form name="rowform" ng-show="rowform.$visible" class="form-buttons form-inline">
								<button type="submit" ng-disabled="rowform.$waiting" class="btn btn-primary btn-xs"><span class="glyphicon glyphicon-floppy-save"></span> 保存</button>
							</form>
							<div class="buttons" ng-show="!rowform.$visible">
								<button class="btn btn-warning btn-xs" ng-if="$index >= topic.partitions.length -1" ng-click="rowform.$show()"><span class="glyphicon glyphicon-edit"></span> 修改</button>
								<button class="btn btn-danger btn-xs"><span class="glyphicon glyphicon-remove"></span> 删除</button>
							</div>
						</td>
					</tr>
				</tbody>
			</table>
			<h4>
				<span class="label label-primary">Consumers</span>
			</h4>
			<hr>
		</div>
	</div>

	<script type="text/javascript" src="${model.webapp}/js/angular.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/angular-resource.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/xeditable.min.js"></script>

	<script type="text/javascript" src="${model.webapp}/js/topic-detail.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/bootbox.min.js"></script>
</a:layout>