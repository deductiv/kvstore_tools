import os
import shutil
import fnmatch
import configparser
# Merge package/default_merge/*.conf with output/<appname>/default/*.conf
# Build process automatically copies default/ files after build, overwriting built files
# jr.murray@deductiv.net 2023-10-06

def additional_packaging(addon_name):
    """ Additional packaging script to run post-build """
    # Glob files in package/default/*.conf
    script_path = os.path.dirname(os.path.abspath(__file__))
    default_config_path = os.path.join(script_path, 'output', addon_name, 'default')
    custom_config_path = os.path.join(script_path, 'package', 'default_merge')

    for file in os.listdir(custom_config_path):
        if fnmatch.fnmatch(file, '*.conf'):
            default_config = configparser.ConfigParser()
            custom_config = configparser.ConfigParser()
            merged_config = configparser.ConfigParser()
            default_config_file = os.path.join(default_config_path, file)
            custom_config_file = os.path.join(custom_config_path, file)
            configs_to_overwrite = {}

            # Define which files, stanzas, and settings to merge
            # Supports wildcards for stanzas
            manual_merges = {
                # Filename > stanzas > [settings]
                'restmap.conf': {
                    '*': ['members']
            }}

            if os.path.isfile(default_config_file):
                if file in list(manual_merges.keys()):
                    # Read both config files
                    default_config.read(default_config_file)
                    custom_config.read(custom_config_file)

                    # Loop through the sections to merge
                    for merge_section in list(manual_merges[file].keys()):
                        # Support wildcards in the dict match for manual_merges
                        for section in fnmatch.filter(default_config.sections(), merge_section):
                            for key in list(default_config[section].keys()):
                                if key in manual_merges[file][merge_section]:
                                    # Merge members with the custom config, overwrite custom config in memory
                                    if section in custom_config.sections() and key in list(custom_config[section].keys()):
                                        
                                        print(f'Found [{section}]/{key} setting in output and custom configs. Merging.')
                                        default_setting = [x.strip() for x in default_config[section][key].split(',')]
                                        custom_setting = [x.strip() for x in custom_config[section][key].split(',')]
                                        merged_setting = ', '.join(default_setting + custom_setting)

                                        # Commit to the custom config dict in memory
                                        if file not in list(configs_to_overwrite.keys()):
                                            configs_to_overwrite[file] = {}
                                        if section not in list(configs_to_overwrite[file].keys()):
                                            configs_to_overwrite[file][section] = {}
                                        configs_to_overwrite[file][section][key] = merged_setting

                # Merge the config files in memory
                merged_config.read([default_config_file, custom_config_file])

                # Overwrite the settings to be merged
                for config_file in list(configs_to_overwrite.keys()):
                    if config_file == file:
                        for config_section in list(configs_to_overwrite[config_file].keys()):
                            for setting, value in list(configs_to_overwrite[config_file][config_section].items()):
                                if config_section not in merged_config.sections():
                                    merged_config.add_section(config_section)
                                merged_config.set(config_section, setting, value)
                                print(f'Set {config_file}/{config_section}/{setting} to {value}')
                
                # Write the configuration file to disk in the output/appname/default folder
                with open(default_config_file, 'w') as merged_config_file:
                    merged_config.write(merged_config_file)
                print('Config merged: ' + file)
            else:
                # Copy the config file. 
                # No merge necessary; this can be moved to package/default.
                shutil.copyfile(custom_config_file, default_config_file)
                print('Config copied: ' + file)

    # Undo the copying of default_merge to the output directory
    mergefolder_output = os.path.join(script_path, 'output', addon_name, 'default_merge')
    if os.path.isdir(mergefolder_output):
        shutil.rmtree(mergefolder_output)
