import os

from setuptools import setup, find_packages

dir_path = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(dir_path, "requirements.txt")) as req_file:
    install_requires = [req.rstrip("\n") for req in req_file.readlines()]

setup(
    name='query_flow',
    packages=find_packages(),
    version='0.1',
    author='Eyal Trabelsi',
    author_email='eyaltrabelsi@gmail.com',
    install_requires=install_requires
)