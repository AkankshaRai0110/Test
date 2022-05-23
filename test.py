from configparser import ConfigParser

file='testconfig.ini'
config= ConfigParser()
config.read(file)
print(config.sections())
print(config['account']['status'])
print(config['client']['user'])
print(config['client']['pwd'])


