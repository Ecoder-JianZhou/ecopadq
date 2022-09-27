#import ez_setup
#ez_setup.use_setuptools()
from setuptools import setup, find_packages
setup(name='ecopadq',
      version='0.3',
      packages= find_packages(),
#      package_data={'ecopadq':['ecopadq/tasks/templates/*.tmpl']},
      install_requires=[
        'paramiko',
#       'dockertask @ git+git://github.com/ou-ecolab/dockertask.git@test#egg=dockertask',
#       'celery',
#       'requests',
#       'jinja2',
      ],
#      dependency_links=[
#          'https://github.com/ouinformatics/dockertask/tarball/master#egg=dockertask-0.0',
#      ],
#      include_package_data=True,
)
