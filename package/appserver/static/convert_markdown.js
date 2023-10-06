/* jshint esversion: 6 */
const currentUrl = window.location.href; 
const app = currentUrl.replace(/.*\/app\/([^\/]+).*/, '$1')
// console.log('app', app)
require.config({
	paths: {
		"markdown-it": 			`/static/app/${app}/lib/markdown-it.min`,
		"markdown-it-sup": 		`/static/app/${app}/lib/markdown-it-sup.min`,
		"markdown-it-footnote": `/static/app/${app}/lib/markdown-it-footnote.min`
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

    var md = require('markdown-it')()
        .use(require('markdown-it-sup'))
        .use(require('markdown-it-footnote'));
    
    // Substitute link paths that work in github with ones that work in Splunk
    var text = $('#markdown').html();
    var static_links_re = /(\(\/static\/)([^)]+\))/g;
    text = text.replace(static_links_re, `$1app/${app}/$2`);
    // console.log('text', text)
    
    var xml_escapes_re = /&amp;(?=lt;|gt;|nbsp;)/g;
    
    var result = md.render(text);
    result = result.replace(xml_escapes_re, '&');
    $('#documentation').html(result);
});