from setuptools import setup, find_packages

VERSION = "1.0.11"

setup(name='doltpy',
      version=VERSION,
      packages=find_packages(),
      install_requires=['pandas>=1.0.3',
                        'mysql-connector-python==8.0.20',
                        'retry>=0.9.2',
                        'psycopg2-binary>=2.8.5',
                        'psutil>=5.7.0',
                        'SQLAlchemy>=1.3.18'],
      tests_require=['pytest-docker>=0.7.2', 'PyYAML', 'pytest'],
      setup_requires=['wheel'],
      author='Liquidata',
      author_email='oscar@liquidata.co',
      description='A Python package for using Dolt database via Python.',
      url='https://github.com/liquidata-inc/doltpy',
      download_url='https://github.com/liquidata-inc/doltpy/archive/v{}.tar.gz'.format(VERSION),
      keywords=['Dolt', 'Liquidata', 'DoltHub', 'ETL', 'ELT'],
      project_urls={'Bug Tracker': 'https://github.com/liquidata-inc/core/issues'},
      entry_points={
            'console_scripts': [
                  'dolthub-load=doltpy.etl:dolthub_loader_main',
                  'dolt-load=doltpy.etl:dolt_loader_main'
            ]
      })
