<%@ page contentType="text/html; charset=utf-8" isELIgnored="false" trimDirectiveWhitespaces="true"%>
<%@ taglib prefix="a" uri="/WEB-INF/app.tld"%>
<jsp:useBean id="ctx" type="com.ctrip.hermes.portal.console.meta.Context" scope="request" />
<jsp:useBean id="payload" type="com.ctrip.hermes.portal.console.meta.Payload" scope="request" />
<jsp:useBean id="model" type="com.ctrip.hermes.portal.console.meta.Model" scope="request" />

<a:layout>
	<link href="${model.webapp}/css/prettydiff.css" type="text/css" rel="stylesheet">
	<div ng-app="hermes-meta" ng-controller="meta-controller" class="row">
		<div class="col-md-offset-2 col-md-8">
			<div class="panel panel-info">
				<div class="panel-heading">
					<label class="radio-inline"> <input type="radio" ng-model="metaStatus" value="oldMeta"> 旧Meta
					</label> <label class="radio-inline"> <input type="radio" ng-model="metaStatus" value="newMeta"> 新Meta
					</label> <label class="radio-inline"> <input type="radio" ng-model="metaStatus" value="diff"> View Diff
					</label>
					<button ng-click="refreshCachedMeta()" class="btn btn-xs btn-success" aria-hidden="true" style="margin-left: 10px">
						<span class="glyphicon glyphicon-refresh"></span> 刷新Meta
					</button>
					<button ng-click="buildMeta()" class="btn btn-xs btn-success pull-right" aria-hidden="true">
						<span class="glyphicon glyphicon-saved"></span> Build Meta
					</button>
				</div>
				<div class="panel-body">
					<textarea ng-show="metaStatus!='diff'" id="meta" class="form-control" rows="35" disabled style="cursor: default">{{meta[metaStatus]|json:4}}</textarea>
					<div ng-show="metaStatus=='diff'">
						<div class="col-md-4">
							<div class="panel panel-default">
								<div class="panel-heading"><span class="glyphicon glyphicon-asterisk"></span> Topic以外更新</div>
								<div class="list-group">
									<a ng-if="metaDiff.apps!=null" href="#" class="list-group-item ng-class:{active:currentDiffDetail=='apps'}" ng-click="prettyDiff(meta.oldMeta.apps,meta.newMeta.apps,'apps')"> apps </a> <a ng-if="metaDiff.codecs!=null" href="#" class="list-group-item ng-class:{active:currentDiffDetail=='codecs'}" ng-click="prettyDiff(meta.oldMeta.codecs,meta.newMeta.codecs,'codecs')"> codecs </a> <a ng-if="metaDiff.endpoints!=null" href="#"
										class="list-group-item ng-class:{active:currentDiffDetail=='endpoints'}" ng-click="prettyDiff(meta.oldMeta.endpoints,meta.newMeta.endpoints,'endpoints')"> endpoints </a> <a ng-if="metaDiff.idcs!=null" href="#" class="list-group-item ng-class:{active:currentDiffDetail=='idcs'}" ng-click="prettyDiff(meta.oldMeta.idcs,meta.newMeta.idcs,'idcs')"> idcs </a> <a ng-if="metaDiff.servers!=null" href="#" class="list-group-item ng-class:{active:currentDiffDetail=='servers'}"
										ng-click="prettyDiff(meta.oldMeta.servers,meta.newMeta.servers,'servers')"> servers </a> <a ng-if="metaDiff.storages!=null" href="#" class="list-group-item ng-class:{active:currentDiffDetail=='storages'}" ng-click="prettyDiff(meta.oldMeta.storages,meta.newMeta.storages,'storages')"> storages </a> <a ng-if="metaDiff.version!=null" href="#" class="list-group-item ng-class:{active:currentDiffDetail=='version'}" ng-click="prettyDiff(meta.oldMeta.version,meta.newMeta.version,'version')">
										version </a> <a ng-if="metaDiff.id!=null" href="#" class="list-group-item ng-class:{active:currentDiffDetail=='id'}" ng-click="prettyDiff(meta.oldMeta.id,meta.newMeta.id,'id')"> id </a>
								</div>
							</div>
							<div class="panel panel-default">
								<div class="panel-heading"><span class="glyphicon glyphicon-asterisk"></span> Topic增加</div>
								<div class="list-group">
									<a ng-repeat="(key,topic) in metaDiff.addedTopics" href="#" class="list-group-item ng-class:{active:currentDiffDetail==topic.name}" ng-click="prettyDiff('',meta.newMeta.topics[topic.name],topic.name)"> {{topic.name}} </a>
								</div>
							</div>
							<div class="panel panel-default">
								<div class="panel-heading"><span class="glyphicon glyphicon-asterisk"></span> Topic删除</div>
								<div class="list-group">
									<a ng-repeat="(key,topic) in metaDiff.removedTopics" href="#" class="list-group-item ng-class:{active:currentDiffDetail==topic.name}" ng-click="prettyDiff(meta.oldMeta.topics[topic.name],'',topic.name)"> {{topic.name}} </a>
								</div>
							</div>
							<div class="panel panel-default">
								<div class="panel-heading"><span class="glyphicon glyphicon-asterisk"></span> Topic修改</div>
								<div class="list-group">
									<a ng-repeat="(key,topic) in metaDiff.changedTopics" href="#" class="list-group-item ng-class:{active:currentDiffDetail==topic.name}" ng-click="prettyDiff(meta.oldMeta.topics[topic.name],meta.newMeta.topics[topic.name],topic.name)"> {{topic.name}} </a>
								</div>
							</div>
						</div>
						<div class="col-md-8 white" id="prettydiff" style="max-height: 700px; overflow: auto;"></div>
					</div>
				</div>
			</div>
		</div>
	</div>
	<script src="${model.webapp}/js/meta/meta.js"></script>
	<script src="${model.webapp}/js/prettydiff/safeSort.js"></script>
	<script src="${model.webapp}/js/prettydiff/diffview.js"></script>
	<script src="${model.webapp}/js/prettydiff/csspretty.js"></script>
	<script src="${model.webapp}/js/prettydiff/csvpretty.js"></script>
	<script src="${model.webapp}/js/prettydiff/jspretty.js"></script>
	<script src="${model.webapp}/js/prettydiff/markuppretty.js"></script>
	<script src="${model.webapp}/js/prettydiff/prettydiff.js"></script>
</a:layout>