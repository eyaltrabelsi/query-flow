import os

from setuptools import find_packages
from setuptools import setup

dir_path = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(dir_path, 'requirements.txt')) as req_file:
    install_requires = [req.rstrip('\n') for req in req_file.readlines()]

with open(os.path.join(dir_path, 'README.md')) as readme_file:
    long_description = readme_file.read()

setup(
    name='query_flow',
    version='0.1',
    description='Understand your Queries using Sankey Diagrams',
    author='Eyal Trabelsi',
    author_email='eyaltrabelsi@gmail.com',
    license='MIT',
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
    ],
    install_requires=install_requires,
    python_requires='>=3.4',
    extras_require={
        'development': [
            'pre-commit==2.1.1', 'pytest==5.2.0', 'tox==3.20.1',
        ],
    },
)
