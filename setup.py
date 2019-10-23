from setuptools import setup, find_packages

setup(name='core',
      version='0.1',
      packages=find_packages(),
      install_requires=['pandas>=0.25.0', 'pyarrow>=0.14.1', 'mysql-connector-python==8.0.17', 'retry>=0.9.2'],
      author='Liquidata',
      author_email='oscar@liquidata.co',
      description='A Python package for using Dolt database via Python.',
      url='https://github.com/liquidata-inc/doltpy',
      keywords=['Dolt', 'Liquidata', 'DoltHub', 'ETL', 'ELT'],
      project_urls={'Bug Tracker': 'https://github.com /liquidata-inc/core/issues'},
      entry_points={
            'console_scripts': [
                  'dolthub-load=doltpy.etl:dolthub_loader_main',
                  'dolt-load=doltpy.etl:dolt_loader_main'
            ]
      })
