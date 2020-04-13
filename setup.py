from setuptools import setup, find_packages

setup(name='doltpy',
      version='0.0.7',
      packages=find_packages(),
      install_requires=['pandas>=0.25.0', 'sqlalchemy>=1.3.8', 'mysql-connector-python==8.0.17', 'retry>=0.9.2'],
      author='Liquidata',
      author_email='oscar@liquidata.co',
      description='A Python package for using Dolt database via Python.',
      url='https://github.com/liquidata-inc/doltpy',
      download_url='https://github.com/liquidata-inc/doltpy/archive/v0.0.5.tar.gz',
      keywords=['Dolt', 'Liquidata', 'DoltHub', 'ETL', 'ELT'],
      project_urls={'Bug Tracker': 'https://github.com /liquidata-inc/core/issues'},
      entry_points={
            'console_scripts': [
                  'dolthub-load=doltpy.etl:dolthub_loader_main',
                  'dolt-load=doltpy.etl:dolt_loader_main'
            ]
      })
