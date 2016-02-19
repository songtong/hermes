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
					<button ng-click="refreshCachedMeta()" class="btn btn-xs btn-success" aria-hidden="true" style="margin-left:10px">
						<span class="glyphicon glyphicon-refresh"></span> 刷新Meta
					</button>
					<button ng-click="buildMeta()" class="btn btn-xs btn-success pull-right" aria-hidden="true">
						<span class="glyphicon glyphicon-saved"></span> Build Meta
					</button>
				</div>
				<div class="panel-body">
					<textarea ng-show="metaStatus!='diff'" id="meta" class="form-control" rows="35" disabled style="cursor: default">{{meta[metaStatus]}}</textarea>
					<div ng-show="metaStatus=='diff'" id="prettydiff" class="white"></div>
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