from setuptools import setup, find_packages
import os

version = '1.0'

setup(name='zeo.memcache',
      version=version,
      description="Memcache version of zodb client cache",
      long_description=open("README.txt").read() + "\n" +
                       open(os.path.join("docs", "HISTORY.txt")).read(),
      classifiers=[
        "Framework :: Plone",
        "Programming Language :: Python",
        ],
      keywords='',
      author='Elizabeth Leddy',
      author_email='elizabeth.leddy@gmail.com',
      url='',
      license='GPL',
      packages=[
        'zeo',
        'zeo.cache',
        'zeo.memcache',
      ],
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          'setuptools',
          'ZODB3>=3.9.5',
          'zope.interface',
          
          # -*- Extra requirements: -*-
      ],
      #'python-memcached'
      entry_points="""
      # -*- Entry points: -*-

      [z3c.autoinclude.plugin]
      target = plone
      """,
      setup_requires=[],
      paster_plugins=[],
      )
