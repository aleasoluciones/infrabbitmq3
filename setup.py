from setuptools import setup, find_packages

setup(name='infrabbitmq',
      version='0.0.1',
      author='Bifer Team',
      description='RabbitMQ Client Infrastructure',
      platforms='Linux',
      packages=find_packages(exclude=['specs', 'integration_specs']),
      install_requires=['pika==1.3.2',
                        'retrying==1.3.3',
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
