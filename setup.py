from setuptools import setup, find_packages

install_requires = [
    'python-dateutil>=2.7.3',
    'pytz>=2015.7',
    'pyzmq>=17.1.2',
    'Sphinx>=1.7.8',
    'sphinx-rtd-theme>=0.4.1',
    'sphinxcontrib-websupport>=1.1.0',
    'tzlocal>=1.5.1',
    'avro-python3>=1.8.2',
    'google-api-python-client>=1.4.2',
    'oauth2client>=1.5.2'
]

setup(name='pipeutils',
      version='1.0',
      description='The pipeutils',
      url='https://github.com/codebolab/pipeutils.git',
      author='code.bo',
      author_email='info@code.bo',
      license='MIT',
      packages=find_packages(),
      zip_safe=False,
      install_requires=install_requires,
)
