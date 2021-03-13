from setuptools import setup

import sqlglot

setup(
    name='sqlglot',
    version=sqlglot.__version__,
    description='An easily customizable SQL parser and transpiler',
    url='https://github.com/tobymao/sqlglot',
    author='Toby Mao',
    author_email='toby.mao@gmail.com',
    license='MIT',
    packages=['sqlglot'],
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: SQL',
        'Programming Language :: Python :: 3 :: Only',
    ],
)
