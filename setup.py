from setuptools import setup, find_packages

setup(name='infrabbitmq',
      version='0.0.1',
      author='Bifer Team',
      description='rabbitmq client infrastructure components for python3',
      platforms='Linux',
      packages=find_packages(exclude=['ez_setup',
                                      'examples',
                                      'tests',
                                      'integration_tests',
                                      'specs'
                                      'integration_specs',
                                      ]
                             ),
      install_requires=['pika==1.1.0',
                        'infcommon',
                        ],
      dependency_links=['git+https://github.com/aleasoluciones/infcommon3.git#egg=infcommon'],
      scripts=['bin/ticker.py',
               'bin/queue_delete.py',
               'bin/queue_unbind.py',
               'bin/event_publisher.py',
               'bin/event_processor.py'
               ]
      )
