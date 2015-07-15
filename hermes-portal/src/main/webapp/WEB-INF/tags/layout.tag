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
<link href="${model.webapp}/css/portal-common.css" type="text/css" rel="stylesheet">
<script src="${model.webapp}/js/jquery-1.11.3.min.js" type="text/javascript"></script>
<script src="${model.webapp}/js/bootstrap.min.js" type="text/javascript"></script>
<script src="${model.webapp}/js/bootstrap3-typeahead.min.js" type="text/javascript"></script>
<script src="${model.webapp}/js/portal-common.js" type="text/javascript"></script>

<script type="text/javascript" src="${model.webapp}/js/angular/angular.min.js"></script>
<script type="text/javascript" src="${model.webapp}/js/angular/angular-resource.min.js"></script>
<script type="text/javascript" src="${model.webapp}/js/angular/ui-bootstrap-tpls-0.13.0.min.js"></script>
<script type="text/javascript" src="${model.webapp}/js/angular/bootbox.min.js"></script>

<script type="text/javascript">
	var contextpath = "${model.webapp}";
</script>

<jsp:invoke fragment="head" />
</head>

<body data-spy="scroll" data-target=".subnav" data-offset="50">
	<div class="navbar navbar-inverse navbar-fixed-top">
		<div class="container-fluid">
			<div class="navbar-header">
				<a href="${model.webapp}/${page.moduleName}" class="navbar-brand">
					Hermes
					<span class="badge">${navBar.environment}</span>
				</a>
			</div>

			<div class="collapse navbar-collapse">
				<ul class="nav navbar-nav">
					<c:forEach var="page" items="${navBar.visiblePages}">
						<c:if test="${page.name == 'dashboard' }">
							<li ${model.page.name == page.name ? 'class="active dropdown"' : 'class="dropdown"'}>
								<a href="http://baidu.com" class="dropdown-toggle" data-toggle="dropdown" role="button" aria-haspopup="true" aria-expanded="false">${page.title}
									<span class="caret"></span>
								</a>
								<ul class="dropdown-menu">
									<li>
										<a href="${model.webapp}/${page.moduleName}/${page.path}">Topic</a>
									</li>
									<li>
										<a href="${model.webapp}/${page.moduleName}/${page.path}?op=broker">Broker</a>
									</li>
									<li>
										<a href="${model.webapp}/${page.moduleName}/${page.path}?op=client">Client</a>
									</li>
								</ul>
							</li>
						</c:if>
						<c:if test="${page.standalone and page.name != 'dashboard'}">
							<li ${model.page.name == page.name ? 'class="active"' : ''}>
								<a href="${model.webapp}/${page.moduleName}/${page.path}">${page.title}</a>
							</li>
						</c:if>
						<c:if test="${not page.standalone and model.page.name == page.name and page.name != 'dashboard'}">
							<li class="active">${page.title}</li>
						</c:if>
					</c:forEach>
				</ul>
			</div>
			<!--/.nav-collapse -->
		</div>
	</div>

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
	<!--/.fluid-container-->
</body>
</html>
