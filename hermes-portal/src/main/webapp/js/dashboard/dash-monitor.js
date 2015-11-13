var monitor = angular.module("dash-monitor-event", [ 'ngResource', 'ui.bootstrap', 'smart-table' ]);

monitor.filter('short', function() {
	return function(input, length) {
		input = input || '';
		length = length || 30;
		input = input.replace(/\\"/g, '"');
		if (input.length <= length) {
			return input;
		}
		out = input.substring(0, length / 2) + " ... " + input.substring(input.length - length / 2);
		return out;
	}
});