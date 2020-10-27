/* jshint esversion: 6 */
require.config({
	paths: {
		"markdown-it": 			"/static/app/kvstore_tools/lib/markdown-it.min",
		"markdown-it-sup": 		"/static/app/kvstore_tools/lib/markdown-it-sup.min",
		"markdown-it-footnote": "/static/app/kvstore_tools/lib/markdown-it-footnote.min"
	}
});

require([
    "underscore",
    "jquery",
    "markdown-it",
    "markdown-it-sup",
    "markdown-it-footnote",
	"splunkjs/mvc/simplexml/ready!"
], function(
	_, //underscore
    $  //jquery
) {
    console.log("JS is working");
    
    var md = require('markdown-it')()
        .use(require('markdown-it-sup'))
        .use(require('markdown-it-footnote'));
    
    var text = $('#markdown').html();
    var anglebracket_re = /&amp;(?=lt;|gt;)/g;
    
    var result = md.render(text);
    result = result.replace(anglebracket_re, '&');
    $('#documentation').html(result);
});