#!/usr/bin/env python

from setuptools import setup

setup(name='tap-wootric',
      version='0.5.4',
      description='Singer.io tap for extracting data from the Wootric API',
      author='Stitch',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_CHANGEME'],
      install_requires=[
          'singer-python==5.9.0',
          'requests==2.31.0',
          'backoff==1.8.0'
      ],
      entry_points='''
          [console_scripts]
          tap-wootric=tap_wootric:main
      ''',
      packages=['tap_wootric'],
      package_data = {
          'tap_wootric/schemas': [
              "decline.json",
              "enduser.json",
              "response.json",
          ]
      },
      include_package_data=True,
)