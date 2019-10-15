from setuptools import setup, find_packages

setup(name='doltpy',
      version='0.1',
      packages=find_packages(),
      install_requires=['pandas>=0.25.0', 'pyarrow>=0.14.1', 'mysql-connector-python==8.0.17', 'retry>=0.9.2'],
      author='Liquidata',
      author_email='oscar@liquidata.co',
      description='',
      url='https://github.com/liquidata-inc/doltpy',
      project_urls={'Bug Tracker': 'https://github.com /liquidata-inc/doltpy/issues'},
      entry_points={
            'console_scripts': [
                  'dolthub-load=doltpy_etl:dolthub_loader_main',
                  'dolt-load=doltpy_etl:dolt_loader_main'
            ]
      })
