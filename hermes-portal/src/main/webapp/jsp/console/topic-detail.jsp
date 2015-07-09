<%@ page contentType="text/html; charset=utf-8" isELIgnored="false" trimDirectiveWhitespaces="true"%>
<%@ taglib prefix="a" uri="/WEB-INF/app.tld"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>


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
					<div class="col-sm-4">
						<div class="form-horizontal">
							<div class="form-group" style="margin: 0">
								<label class="control-label col-sm-4 detail-label">编码类型</label>
								<span ng-bind='topic.codecType' editable-select="topic.codecType" style="line-height: 3;" e-ng-options="codec for codec in codec_types"></span>
							</div>
							<div class="form-group" style="margin: 0">
								<label class="control-label col-sm-4 detail-label">存储类型</label>
								<span ng-bind='topic.storageType' style="line-height: 3"></span>
							</div>
							<div class="form-group" style="margin: 0">
								<label class="control-label col-sm-4 detail-label">Endpoint</label>
								<span ng-bind='topic.endpointType' style="line-height: 3"></span>
							</div>
							<div class="form-group" style="margin: 0">
								<label class="control-label col-sm-4 detail-label">ACK 超时</label>
								<span ng-bind='topic.ackTimeoutSeconds' editable-range="topic.ackTimeoutSeconds" style="line-height: 3" e-step="5" e-min="5" e-max="200"></span>
							</div>
						</div>
					</div>
					<div class="col-sm-4">
						<div class="form-horizontal">
							<div class="form-group" style="margin: 0">
								<label class="control-label col-sm-3 detail-label">重试策略</label>
								<span ng-bind='topic.consumerRetryPolicy' editable-text="topic.consumerRetryPolicy" style="line-height: 3"></span>
							</div>
							<div class="form-group" style="margin: 0">
								<label class="control-label col-sm-3 detail-label">创建者</label>
								<span ng-bind='topic.createBy || "None"' editable-text="topic.createBy" style="line-height: 3"></span>
							</div>
							<div class="form-group" style="margin: 0">
								<label class="control-label col-sm-3 detail-label">状态</label>
								<span style="line-height: 3" ng-bind="topic.status"></span>
							</div>
						</div>
					</div>
					<div class="col-sm-4">
						<div class="form-horizontal">
							<div class="form-group" style="margin: 0">
								<label class="control-label col-sm-3 detail-label">创建时间</label>
								<span style="line-height: 3" ng-bind="topic.createTime"></span>
							</div>
							<div class="form-group" style="margin: 0">
								<label class="control-label col-sm-3 detail-label">修改时间</label>
								<span style="line-height: 3" ng-bind="topic.lastModifiedTime"></span>
							</div>
							<div class="form-group" style="margin: 0">
								<label class="control-label col-sm-3 detail-label">描述</label>
								<span ng-bind='topic.description || "None"' editable-text="topic.description" style="line-height: 3"></span>
							</div>
						</div>
					</div>
				</div>
			</form>
			<h4>
				<span class="label label-primary">Informations</span>
			</h4>
			<hr>
			<div class="container row">
				<div class="col-md-6">
					<div class="panel panel-success">
						<div class="panel-heading">
							<span class="label label-danger">生产</span>
							<button type="button" data-toggle="modal" data-target="#top-producer-modal" class="btn btn-xs btn-success" style="text-align: center;"><span class="glyphicon glyphicon-th-lists"></span>
								详情</button>
						</div>
						<div class="panel-body">
							<iframe style="border: 0"
								src="${model.kibanaUrl}/#/visualize/edit/Message-Received?embed&_g=(refreshInterval:(display:'10%20seconds',pause:!f,section:1,value:5000),time:(from:now-1h,mode:quick,to:now))&_a=(filters:!(),linked:!f,query:(query_string:(analyze_wildcard:!t,query:'eventType:Message.Received')),vis:(aggs:!((id:'1',params:(),schema:metric,type:count),(id:'3',params:(customInterval:'2h',extended_bounds:(),field:'@timestamp',interval:auto,min_doc_count:1),schema:segment,type:date_histogram),(id:'2',params:(filters:!((input:(query:(query_string:(analyze_wildcard:!t,query:'datas.topic:${model.topicName}')))))),schema:group,type:filters)),listeners:(),params:(addLegend:!t,addTimeMarker:!f,addTooltip:!t,defaultYExtents:!f,mode:stacked,scale:linear,setYExtents:!f,shareYAxis:!t,times:!(),yAxis:()),type:histogram))"
								height="200" width="500"></iframe>
						</div>
					</div>
				</div>
				<div class="col-md-6">
					<div class="panel panel-success">
						<div class="panel-heading">
							<span class="label label-danger">生产速率(分钟)</span>
						</div>
						<div class="panel-body">
							<iframe style="border: 0"
								src="${model.kibanaUrl}/#/visualize/edit/Produced-slash-min?embed&_g=(refreshInterval:(display:'10%20seconds',pause:!f,section:1,value:10000),time:(from:now-1m,mode:relative,to:now))&_a=(filters:!(),linked:!f,query:(query_string:(analyze_wildcard:!t,query:'datas.topic:${model.topicName}%20AND%20eventType:Message.Received')),vis:(aggs:!((id:'1',params:(json:''),schema:metric,type:count)),listeners:(),params:(fontSize:'50'),type:metric))"
								height="200" width="500"></iframe>
						</div>
					</div>
				</div>
			</div>
			<div class="container row">
				<c:forEach var="consumer" items="${model.consumers}">
					<div class="col-md-6">
						<div class="panel panel-success">
							<div class="panel-heading">
								<span class="label label-danger">消费：${consumer}</span>
							</div>
							<div class="panel-body">

								<iframe style="border: 0"
									src="${model.kibanaUrl}/#/visualize/edit/Message-Acked?embed&_g=(refreshInterval:(display:'10%20seconds',pause:!f,section:1,value:10000),time:(from:now-1h,mode:quick,to:now))&_a=(filters:!(),linked:!f,query:(query_string:(analyze_wildcard:!t,query:'eventType:Message.Acked')),vis:(aggs:!((id:'1',params:(),schema:metric,type:count),(id:'2',params:(customInterval:'2h',extended_bounds:(),field:'@timestamp',interval:auto,min_doc_count:1),schema:segment,type:date_histogram),(id:'3',params:(filters:!((input:(query:(query_string:(analyze_wildcard:!t,query:'datas.groupId:${consumer}')))))),schema:group,type:filters)),listeners:(),params:(addLegend:!t,addTimeMarker:!f,addTooltip:!t,defaultYExtents:!f,mode:stacked,scale:linear,setYExtents:!f,shareYAxis:!t,times:!(),yAxis:()),type:histogram))"
									height="200" width="500"></iframe>
							</div>
						</div>
					</div>
					<div class="col-md-6">
						<div class="panel panel-success">
							<div class="panel-heading">
								<span class="label label-danger">${consumer} 速率(分钟)</span>
							</div>
							<div class="panel-body">
								<iframe style="border: 0"
									src="${model.kibanaUrl}/#/visualize/edit/Consumed-slash-min?embed&_g=(refreshInterval:(display:'10%20seconds',pause:!f,section:1,value:10000),time:(from:now-1m,mode:relative,to:now))&_a=(filters:!(),linked:!f,query:(query_string:(analyze_wildcard:!t,query:'datas.topic:${model.topicName}%20AND%20datas.groupId:${consumer}%20AND%20eventType:Message.Acked')),vis:(aggs:!((id:'1',params:(),schema:metric,type:count)),listeners:(),params:(fontSize:60),type:metric))"
									height="200" width="500"></iframe>
							</div>
						</div>
					</div>
				</c:forEach>
			</div>

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
		<div class="modal fade" id="top-producer-modal" tabindex="-1" role="dialog" aria-labelledby="top-producer-label" aria-hidden="true">
			<div class="modal-dialog">
				<div class="modal-content">
					<div class="modal-header">
						<button type="button" class="close" data-dismiss="modal" aria-label="Close"><span aria-hidden="true">&times;</span></button>
						<h4 class="modal-title" id="top-producer-label">Top 100 Producers</h4>
					</div>
					<div class="modal-body">
						<iframe style="border: 0"
							src="${model.kibanaUrl}/#/visualize/edit/Top-Producer?embed&_g=(filters:!(),refreshInterval:(display:'10%20seconds',pause:!f,section:1,value:10000),time:(from:now-1m,mode:relative,to:now))&_a=(filters:!(),linked:!f,query:(query_string:(analyze_wildcard:!t,query:'eventType:Message.Received%20AND%20datas.topic:${model.topicName}')),vis:(aggs:!((id:'1',params:(),schema:metric,type:count),(id:'2',params:(field:datas.producerIp.raw,order:desc,orderBy:'1',size:100),schema:bucket,type:terms)),listeners:(),params:(perPage:10,showMeticsAtAllLevels:!f,showPartialRows:!f),type:table))"
							width="100%" height="400"></iframe>
					</div>
				</div>
			</div>
		</div>
	</div>

	<script type="text/javascript" src="${model.webapp}/js/angular.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/angular-resource.min.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/xeditable.min.js"></script>

	<script type="text/javascript" src="${model.webapp}/js/topic-detail.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/bootbox.min.js"></script>
</a:layout>