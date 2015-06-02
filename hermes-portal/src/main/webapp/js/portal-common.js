var show_op_info = function() {
	"use strict";

	var info_elem, hideHandler, that = {};
	that.init = function(options) {
		info_elem = $(options.selector);
	};
	that.show = function(text) {
		clearTimeout(hideHandler);
		info_elem.find("span").html(text);
		info_elem.delay(200).fadeIn().delay(4000).fadeOut();
	};
	return that;
}();

$(function() {
	show_op_info.init({
		"selector" : ".op-alert"
	});
});
