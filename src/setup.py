from setuptools import setup, find_packages  # type: ignore

setup(name='CSToolkit',
      version='0.1.0-RELEASE',
      packages=find_packages(),
      install_requires=['pytest', 'pytest-mypy', 'mypy'])
