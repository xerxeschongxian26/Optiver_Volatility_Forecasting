"""
Insert module description
"""

from configparser import ConfigParser


def config_for_database_connection(file_name="config/database.ini",
                                   section="postgresql"):
    # create a parser object
    parser = ConfigParser()
    # read config file
    parser.read(file_name)
    config_dict = dict()
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config_dict[param[0]] = param[1]
    else:
        raise Exception('Section {0} is not found in {1} file'.format(section, file_name))
    return config_dict


def config_for_data_paths(file_name="config/database.ini",
                          section="data_path"):
    # create a parser object
    parser = ConfigParser()
    # read config file
    parser.read(file_name)
    config_dict = dict()
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config_dict[param[0]] = param[1]
    else:
        raise Exception('Section {0} is not found in {1} file'.format(section, file_name))
    return config_dict


if __name__ == '__main__':
    print(config())