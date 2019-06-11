# setup for finddup package

import os.path
from setuptools import setup

here = os.path.abspath(os.path.dirname(__file__))

setup(
        name='finddup',
        version='0.1',
        description='disk usage with ranking',
        author='Matthew Clapp',
        author_email='itsayellow+dev@gmail.com',
        license='MIT',
        classifiers=[
            'Development Status :: 3 - Alpha',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: MIT License',
            'Programming Language :: Python :: 3'
            ],
        keywords='du ranking usage',
        py_modules=['finddup'],
        install_requires=[
            'tictoc @ git+https://github.com/itsayellow/tictoc@master',
            ],
        entry_points={
            'console_scripts':[
                'finddup=finddup:cli'
                ]
            },
        python_requires='>=3',
        )

