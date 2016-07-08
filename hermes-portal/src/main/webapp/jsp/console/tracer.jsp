<%@ page contentType="text/html; charset=utf-8" isELIgnored="false" trimDirectiveWhitespaces="true"%>
<%@ taglib prefix="a" uri="/WEB-INF/app.tld"%>
<jsp:useBean id="ctx" type="com.ctrip.hermes.portal.console.tracer.Context" scope="request" />
<jsp:useBean id="payload" type="com.ctrip.hermes.portal.console.tracer.Payload" scope="request" />
<jsp:useBean id="model" type="com.ctrip.hermes.portal.console.tracer.Model" scope="request" />

<a:layout>
	<script>
		var esUrl = "${model.esHost}";
	</script>

	<div class="container" ng-app="hermes-tracer" ng-controller="tracer-controller" style="margin-top: 20px">
		<div class="form-group form-inline" align="center" style="margin-buttom:10px">
			<label>TopicName</label> <input type="text" class="form-control" style="width: 200px" ng-model="topicName" /> <label>RefKey</label> <input type="text" class="form-control" style="width: 300px" ng-model="refKey" /> <input id="msgDate" type="date" class="form-control" ng-model="msgDate" placeholder="yyyy-MM-dd" />
			<button class="btn btn-success" ng-click="trace(topicName,refKey,msgDate)">
				<span class="glyphicon glyphicon-arrow-up"></span> 提交
			</button>
			<label>{{hintWord}}</label>
		</div>
		<div class="panel panel-default" ng-repeat="mlc in mlcs">
			<div class="panel-body">
				<h4>partition:{{mlc.partition}}, msgId:{{mlc.msgId}}, priority:{{mlc.priority}}</h4>
				<hr>
				<div class="alert alert-info" style="margin-bottom: 0px;">
					<strong>生产阶段</strong>(产生时间:{{mlc.produce.born|date:'MM-dd HH:mm:ss'}}, 生产者Ip:{{mlc.produce.producerIp}})
				</div>
				<table class="table table-striped" style="font-size: small;" ng-repeat="produce in mlc.produce.tryProduces track by $index">
					<thead>
						<tr>
							<th>发送次数</th>
							<th>brokerIp</th>
							<th>hermes接收时间</th>
							<th>保存数据库时间</th>
						</tr>
					</thead>
					<tbody>
						<tr>
							<td>{{produce.times}}</td>
							<td>{{produce.brokerIp}}</td>
							<td ng-if="produce.receive==null">-</td>
							<td ng-if="produce.receive!=null">{{produce.receive.eventTime|date:'MM-dd HH:mm:ss'}}</td>
							<td ng-if="produce.save==null">-</td>
							<td ng-if="produce.save!=null">{{produce.save.eventTime|date:'MM-dd HH:mm:ss'}}(<span ng-if="produce.save.success" class="text-success">成功</span><span ng-if="!produce.save.success" class="text.danger"><strong>失败</strong></span>)
							</td>
						</tr>
					</tbody>
				</table>
				<hr>
				<div class="alert alert-success">
					<strong>消费阶段</strong>
				</div>
				<div ng-repeat="consume in mlc.consume track by $index">
					<div>
						<strong>消费者:{{consume.name}}, id:{{consume.id}}</strong>
					</div>
					<table class="table table-striped" style="font-size: small;">
						<thead>
							<tr>
								<th>投递次数</th>
								<th>consumerIp</th>
								<th>brokerIp</th>
								<th>是否重发？</th>
								<th>投递时间</th>
								<th>Ack时间</th>
							</tr>
						</thead>
						<tbody>
							<tr ng-repeat="tryConsume in consume.tryConsumes track by $index">
								<td>{{tryConsume.times}}</td>
								<td>{{tryConsume.consumerIp}}</td>
								<td>{{tryConsume.brokerIp}}</td>
								<td><span ng-if="!tryConsume.isResend" class="text-success">否</span><span ng-if="tryConsume.isResend" class="text-danger">是</span><span ng-if="tryConsume.isResend">(重发Id：{{tryConsume.resendId}})</span></td>
								<td>{{tryConsume.deliver.eventTime|date:'MM-dd HH:mm:ss'}}</td>
								<td><div ng-repeat="ack in tryConsume.acks track by $index">
										{{ack.eventTime|date:'MM-dd HH:mm:ss'}}(<span ng-if="ack.success" class="text-success">成功</span><span ng-if="!ack.success" class="text-danger"><strong>失败</strong></span>) 客户端处理时间：<a href="" tooltip="{{ack .bizProcessStartTime|date:'MM-dd HH:mm:ss'}} ~ {{ack.bizProcessEndTime|date:'MM-dd HH:mm:ss'}}"><span>{{ack.bizProcessEndTime-ack.bizProcessStartTime}}ms </span>
										
										</a>
									</div></td>
							</tr>
						</tbody>
					</table>
				</div>
			</div>
		</div>
		<alert-x id="alert" title="提醒"></alert-x>
	</div>


	<script type="text/javascript" src="${model.webapp}/js/d3/d3.min.js" type="text/JavaScript"></script>
	<script type="text/javascript" src="${model.webapp}/js/highcharts/highcharts.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/highcharts/highcharts-more.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/highcharts/exporting.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/lib/global.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/lib/utils.js"></script>
	<script type="text/javascript" src="${model.webapp}/js/lib/components.js"></script>
	<script src="${model.webapp}/js/tracer/tracer.js"></script>


</a:layout>