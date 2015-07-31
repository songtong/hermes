var log_in_module = angular.module('log-in-app', [ 'ngCookies', 'ngResource' ]);

log_in_module.controller('log-in-controller', [
		'$scope',
		'$resource',
		'$window',
		'$cookies',
		function($scope, $resource, $window, $cookies) {
			var accountResource = $resource("/api/account", {}, {
				login : {
					url : "/api/account/login",
					method : "POST",
					params : {
						name : '@name',
						password : '@password'
					}
				}
			});

			$scope.userinfo = {
				userName : "",
				password : ""
			};
			$scope.isLogin = false;

			$scope.login = function login(userinfo) {
				if (userinfo.userName == "" || userinfo.password == "") {
					show_op_info.show("User name or password can not be null!", false);
				} else {
					accountResource.login({
						name : userinfo.userName,
						password : userinfo.password
					}, function(resp_success) {
						show_op_info.show("Login success!", true);
						$window.location.reload();
					}, function(resp_failed) {
						show_op_info.show("Login failed: " + resp_failed.data,
								false);
					});
				}
			}
			$scope.logout = function logout() {
				$cookies.remove('_token', {
					path : "/"
				});
				$window.location = "/console";
				show_op_info.show("Logout success!", true);
			}
		} ]);
angular.bootstrap($("#login-app"), [ "log-in-app" ]);