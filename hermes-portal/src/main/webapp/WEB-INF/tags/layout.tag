<%@ tag isELIgnored="false" trimDirectiveWhitespaces="true"%>
<%@ attribute name="head" fragment="true" required="false"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<jsp:useBean id="navBar" class="com.ctrip.hermes.portal.view.NavigationBar" scope="page" />

<!DOCTYPE html>
<html lang="en">

<head>
<title>Portal - ${model.page.description}</title>
<meta charset="utf-8">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta http-equiv="content-type" content="text/html; charset=UTF-8" />
<meta name="description" content="Portal">
<link href="${model.webapp}/css/bootstrap.min.css" type="text/css" rel="stylesheet">

<link href="${model.webapp}/css/jquery-ui.min.css" type="text/css" rel="stylesheet">
<link href="${model.webapp}/css/bootstrap-tokenfield.min.css" type="text/css" rel="stylesheet">
<link href="${model.webapp}/css/typeahead.css" type="text/css" rel="stylesheet">


<link href="${model.webapp}/css/portal-common.css" type="text/css" rel="stylesheet">
<link href="${model.webapp}/css/btnUpload.min.css" type="text/css" rel="stylesheet">

<script src="${model.webapp}/js/jquery-2.1.4.min.js" type="text/javascript"></script>

<script src="${model.webapp}/js/jquery-ui.min.js" type="text/javascript"></script>
<script src="${model.webapp}/js/bootstrap-tokenfield.min.js" type="text/javascript"></script>
<script src="${model.webapp}/js/typeahead.bundle.min.js" type="text/javascript"></script>

<script src="${model.webapp}/js/bootstrap.min.js" type="text/javascript"></script>
<script src="${model.webapp}/js/portal-common.js" type="text/javascript"></script>

<script type="text/javascript" src="${model.webapp}/js/angular/angular.min.js"></script>
<script type="text/javascript" src="${model.webapp}/js/angular/angular-resource.min.js"></script>
<script type="text/javascript" src="${model.webapp}/js/angular/angular-cookies.min.js"></script>
<script type="text/javascript" src="${model.webapp}/js/angular/ui-bootstrap-tpls-0.13.0.min.js"></script>
<script type="text/javascript" src="${model.webapp}/js/angular/bootbox.min.js"></script>

<script type="text/javascript">
	var contextpath = "${model.webapp}";
</script>

<jsp:invoke fragment="head" />
</head>

<body data-spy="scroll" data-target=".subnav" data-offset="50">


	<div class="container-fluid" style="min-height: 524px;">
		<div class="row-fluid">
			<div class="span12">
				<div class="op-alert" role="alert" style="display: none;">
					<span id="op_info" style="line-height: 1.8">The examples populate this alert with dummy content</span>
				</div>
				<jsp:doBody />
			</div>
		</div>

		<br />
		<div class="container">
			<footer>
				<center>&copy;2015 Hermes Team</center>
			</footer>
		</div>
	</div>
	<div id="login-app" ng-app="log-in-app" ng-controller="log-in-controller">
		<div class="navbar navbar-inverse navbar-fixed-top">
			<div class="container-fluid">
				<div class="navbar-header">
					<a href="${model.webapp}/${page.moduleName}" class="navbar-brand"> Hermes <span class="badge">${navBar.environment}</span>
					</a>
				</div>

				<div class="collapse navbar-collapse">
					<ul class="nav navbar-nav">
						<c:forEach var="page" items="${requestScope.logined ? navBar.allPages : navBar.basePages}">
							<c:if test="${page.name == 'topic' }">
								<li ${model.page.name == page.name ? 'class="active"' : ''}><a href="${model.webapp}/${page.moduleName}/${page.path}#/list/mysql">${page.title}</a></li>
							</c:if>
							<c:if test="${page.name == 'dashboard' }">
								<li ${model.page.name == page.name ? 'class="active dropdown"' : 'class="dropdown"'}><a href="http://baidu.com" class="dropdown-toggle" data-toggle="dropdown" role="button"
									aria-haspopup="true" aria-expanded="false">${page.title} <span class="caret"></span>
								</a>
									<ul class="dropdown-menu">
										<li><a href="${model.webapp}/${page.moduleName}/${page.path}#/detail/_default">Topic</a></li>
										<li><a href="${model.webapp}/${page.moduleName}/${page.path}?op=broker">Broker</a></li>
										<li><a href="${model.webapp}/${page.moduleName}/${page.path}?op=client">Client</a></li>
									</ul></li>
							</c:if>
							<c:if test="${page.standalone and page.name != 'dashboard' and page.name != 'topic'}">
								<li ${model.page.name == page.name ? 'class="active"' : ''}><a href="${model.webapp}/${page.moduleName}/${page.path}">${page.title}</a></li>
							</c:if>
							<c:if test="${not page.standalone and model.page.name == page.name and page.name != 'dashboard'}">
								<li class="active">${page.title}</li>
							</c:if>
						</c:forEach>
					</ul>
					<ul class="nav navbar-nav navbar-right">
						<c:if test="${requestScope.logined}">
							<li><a class="btn btn-link" ng-click="logout()">Sign out</a></li>
						</c:if>
						<c:if test="${!requestScope.logined}">
							<li><a class="btn btn-link" data-toggle="modal" data-target="#sign_in_modal">Sign in</a></li>
						</c:if>
					</ul>

				</div>
				<!--/.nav-collapse -->
			</div>

		</div>
		<div id="sign_in_modal" class="modal fade" tabindex="-1" role="dialog" aria-labelledby="myModalLabel" aria-hidden="true">
			<div class="modal-dialog" role="document">
				<div class="modal-content">
					<div class="modal-header">
						<button type="button" class="close" data-dismiss="modal" aria-hidden="true">&times;</button>
						<h3 id="myModalLabel">Sign In</h3>
					</div>
					<div class="modal-body">
						<form class="form-horizontal" ng-submit="login(userinfo)">
							<div class="form-group">
								<label for="inputUserName" class="col-sm-3 control-label">User Name</label>
								<div class="col-sm-8">
									<input type="text" class="form-control" id="inputUserName" data-provide="typeahead" placeholder="User Name" ng-model="userinfo.userName">
								</div>
							</div>
							<div class="form-group">
								<label for="inputPassword" class="col-sm-3 control-label">Password</label>
								<div class="col-sm-8">
									<input type="password" class="form-control" id="inputPassword" placeholder="Password" ng-model="userinfo.password">
								</div>
							</div>
							<div class="form-group">
								<input type="reset" class="col-sm-offset-8 btn btn-default" data-dismiss="modal" value="close">&emsp; <input type="submit" class="btn btn-primary" value="sign in">
							</div>
						</form>
					</div>

				</div>
			</div>
		</div>
	</div>
	<!--/.fluid-container-->
	<script type="text/javascript" src="${model.webapp}/js/login/login.js"></script>
</body>
</html>
