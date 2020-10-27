/* jshint esversion: 6 */
require([
	"splunkjs/mvc",
	"splunkjs/mvc/utils",
	"splunkjs/mvc/tokenutils",
	"underscore",
	"jquery",
	"splunkjs/mvc/simplesplunkview",
	"models/SplunkDBase",
	"splunkjs/mvc/sharedmodels",
	"splunkjs/mvc/simplexml",
	"splunkjs/mvc/tableview",
	"splunkjs/mvc/chartview",
	"splunkjs/mvc/searchmanager",
	"splunkjs/mvc/dropdownview",
	"splunkjs/mvc/textinputview",
	"splunkjs/mvc/multidropdownview",
	"splunk.util",
	"splunkjs/mvc/simplexml/element/single",
	"splunkjs/mvc/simpleform/formutils",
	"splunkjs/mvc/simplexml/ready!"
], function(
	mvc,
	utils,
	TokenUtils,
	_, //underscore
	$, //jquery
	SimpleSplunkView,
	SplunkDModel, //SplunkDBase
	sharedModels, //sharedmodels
	DashboardController, //simplexml
	TableView,
	ChartView,
	SearchManager,
	DropdownView,
	TextInputView,
	MultiDropdownView,
	splunkUtil,
	SingleElement,
	FormUtils
) {
	app = 'kvstore_tools';
	
	// Date.now workaround for IE8
	if (!Date.now) {
		Date.now = function() { return new Date().getTime(); };
	}
	
	function epoch_convert(epoch_timestamp) {
		var date = new Date(epoch_timestamp*1000);
		var iso = date.toISOString().match(/(\d{4}\-\d{2}\-\d{2})T(\d{2}:\d{2}:\d{2})/);
		return iso[1] + ' ' + iso[2];
	}
	
	// jQuery element is empty
	function isEmpty( el ){
		return !$.trim(el.html());
	}

	function addMultiLineBreaks(key, value) {
		var replaced_value = value.toString() + '<br/>';
		return replaced_value;
	}
	
	function bool(value) {
		if (value === undefined){
			return false;
		} else if (typeof(value) == 'string'){
			value = value.toLowerCase();
		}
		switch(value){
			case true:
			case "true":
			case 1:
			case "1":
			case "on":
			case "yes":
				return true;
			default: 
				return false;
		}
	}
	function boolFlip(value) {
		if ( bool(value) ) {
			return false;
		} else {
			return true;
		}
	}
	function checkedIfTrue(value) {
		if ( bool(value) ) {
			return "checked";
		} else {
			return "";
		}
	}
	function stringOrArrayToArray(value) {
		if ( value !== undefined && value !== null && value.constructor !== Array ) {
			return [value];
		} else if ( value !== undefined && value !== null && value.constructor === Array ) {
			// value is an array already
			return value;
		} else {
			return null;
		}
	}
	function range(size, startAt = 0) {
		return [...Array(size).keys()].map(i => i + startAt);
	}

	function assign_value_to_element(id, v) {
		//console.log('Checked: ' + $('#' + id).prop('checked'));
		if (id.startsWith('credential')) {
			// Parse the credential
			var [hostname, username, password] = v.split(':');
			if (hostname != undefined && username != undefined && password != undefined ) {
				if ( hostname.length = 0 ) {
					hostname = id;
				}
				var option_html = `<option value="${id}">${username}@${hostname}</option>`;
				// Append the option to the select box
				$('#cred_id').append(option_html);

				// Find the minimum unused ID # for the next credential to be set
				for ( var c=1; c<=20; c++ ) {
					cs = 'credential' + c.toString();
					// If this credential is not set for either config
					if (!( new_settings[cs] || settings[cs] )) {
						//console.log(cs + ' is not set');
						next_credential = cs;
						break;
					}
				}
			}
		} else if (checkbox_ids.includes(id)) {
			$('#' + id).prop('checked', bool(v));
		} else {
				$('#' + id).val(v);
		}
	}

	// Load the configuration and populate the form fields
	var uri = Splunk.util.make_url(`/splunkd/__raw/servicesNS/nobody/${app}/kvst/kvst_config?output_mode=json`);
	var settings = {};
	var new_settings = {};
	var next_credential;
	var credential_num_pattern = /[0-9]{1,2}$/;
	var checkboxes;
	var checkbox_ids = [];

	// Get the checkbox IDs so we can see if an ID is a checkbox when assigning values
	checkboxes = $('input[type=checkbox]').toArray();
	checkboxes.forEach(function(cb) {
		checkbox_ids.push(cb.id);
	});


	function populate_form() {
		var key;
		// Populate the form values
		for (key in settings) {
			if (settings[key] != undefined && settings[key].length > 0) {
				//console.log('settings ' + key + ' = ' + settings[key]);
				assign_value_to_element(key, settings[key]);
			}
		}
		if ( $('#cred_id').children('option').length == 0 ) {
			next_credential = "credential1";
		} else {
			// Find the minimum unused ID # for the next credential to be set
			for ( var c=1; c<=20; c++ ) {
				cs = 'credential' + c.toString();
				// If this credential is not set for either config
				if (!( settings[cs] )) {
					next_credential = cs;
					break;
				}
			}
		}
	}

	$.get(uri, function (result) {
		console.log('Result = ' + JSON.stringify(result));
		// Translate the values from the request
		var config_stanzas = result.entry;
		// Assign the values to config{}
		config_stanzas.forEach(function(entry) {
			if ( entry.name == 'settings' ) {
				settings = entry.content;
			} 
			//config[entry.name] = entry.content;
		});
		// Populate the form values
		populate_form();
	}) .fail( function(e) { 
		//if(e.status == 404){}
		console.log("Failed with error: " + e.status); 
		console.log(e.responseJSON.messages[0].text);
		$('#settings_message').html(e.responseJSON.messages[0].text);
		$('#settings_message').show();
	}); // end get/fail


	// Clicked the credential modify button
	$(document).on('click', '#credential_modify', function(event) {
		// Load the credential fields into the form
		var cred_key = $( "#cred_id" ).val();
		cred = settings[cred_key];
		//console.log(cred);
		var [hostname, username, password] = cred.split(':');

		// Fill the HTML form data
		$('#cred_hostname').val(hostname);
		$('#cred_username').val(username);
		$('#cred_password').val('****************');
		// Set the hidden form value to the cred_key (e.g. credential1)
		$('#cred_id_hidden').val(cred_key);
	});

	// Clicked the credential delete button
	$(document).on('click', '#credential_delete', function(event) {
		// Load the credential fields from the form
		var cred_key = $( "#cred_id" ).val();
		cred = settings[cred_key];
		//console.log(cred);
		//var [hostname, username, password] = cred.split(':');

		new_settings[cred_key] = '';
		//console.log(new_settings);
		// Delete the option from the #cred_id select box
		$( `#cred_id option[value="${cred_key}"]`).remove();
	});

	$(document).on("click", "#setup_save", function(event){
		// Button clicked
		console.log("Saving configuration changes");
		
		checkboxes = $('input[type=checkbox]').toArray();
		texts = $('input[type=text], input[type=password], #log_level').toArray();
		hiddens = $('input[type=hidden]').toArray();
		
		// See if a new credential is being added
		if ($('#cred_hostname').val() != undefined && $('#cred_hostname').val().length > 0) {
			// We have some data
			if ($('#cred_id_hidden').val() != undefined && $('#cred_id_hidden').val().length > 0) {
				// We are modifying a credential entry
				cred_key = $('#cred_id_hidden').val();
			} else {
				// This is a new entry
				cred_key = next_credential;
			}
			asterisk_pattern = /^\*+$/;
			var password;
			if ($('#cred_password').val() != undefined && $('#cred_password').val().match(asterisk_pattern) == null) {
				// We have a newly entered value for password
				password = $('#cred_password').val().trim();
			} else {
				// Use the one from the originally downloaded config
				password = settings[cred_key].split(':')[2];
			}
			// Concatenate the 3 values to make the credential
			new_cred = $('#cred_hostname').val().trim() + ':' + $('#cred_username').val().trim() + ':' + password;
			// See if this is the same as configured before
			if ( new_cred != settings[cred_key] ) {
				new_settings[cred_key] = new_cred;
			}

			if ( settings[cred_key] != undefined ) {
				original_hostname = settings[cred_key].split(':')[0];
			}

			// Clear the form fields
			cred_fields = ['cred_hostname', 'cred_username', 'cred_password', 'cred_id_hidden'];
			cred_fields.forEach(function(c) {
				$('#' + c).val('');
			});

			// Update the select box option
			$(`#cred_id option[value="${cred_key}"]`).remove();
			assign_value_to_element(cred_key, new_cred);
		}

		settings_fields = ['log_level', 'default_path', 'backup_batch_size', 'compression', 'retention_days', 'retention_size'];
		
		checkboxes.forEach(function(f) {
			id = f.id;
			if ( settings_fields.includes(id) ) {
				val = $('#' + id).prop('checked');
				// Check to see if the configuration has changed
				if ( settings[id] != undefined ) {
					if ( bool(val) != bool(settings[id]) ) {
						// If there has been a change, add to the new config
						new_settings[id] = val;
					}
				}
			}
		});
		
		texts.forEach(function(f) {
			id = f.id;
			if ( settings_fields.includes(id) ) {
				val = $('#' + id).val();
				// Check to see if the configuration has changed
				if ( val != settings[id] && (val.length > 0 || settings[id] > 0 )) {
					new_settings[id] = val;
				}
			}
		});

		if (Object.keys(new_settings).length > 0 ) {
			// Submit settings
			var uri = Splunk.util.make_url(`/splunkd/__raw/servicesNS/nobody/${app}/kvst/kvst_config/settings`);
			console.log('Applying new settings: ' + JSON.stringify(new_settings));
			$.ajax({
				url: uri,
				type: 'POST',
				headers: {'Content-Type': 'application/x-www-form-urlencoded'},
				data: $.param(new_settings),
				success: function (data) {
					//console.log(data);
					// Update the screen to show this was successful
					$('#settings_message').html('Configuration update successful.');
					$('#settings_message').show();
					$('html, body').animate({ scrollTop: 0 }, 'fast');
					// Fade out after 5 seconds
					setTimeout(function(){ $('#settings_message').fadeOut(); }, 5000);
				},
				error: function(e) {
					console.log(e);
					// Update the screen to show this didn't work
					$('#settings_message').html('Configuration update failed: ' + JSON.stringify(e));
					$('#settings_message').show();
					$('html, body').animate({ scrollTop: 0 }, 'fast');
				},
				cache: false,
				contentType: false,
				processData: false
			});
		}

		// Apply new_settings to settings
		for (var key1 in new_settings) {
			settings[key1] = new_settings[key1];
		}

		new_settings = {};
		// Refresh the #cred_id select box
		$('#cred_id').empty();
		populate_form();
		
	}); // end save button click function

});