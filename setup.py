# -*- coding:utf-8 -*-

from setuptools import setup

setup(
    name='parade-manage',
    version='0.0.1.14',
    author='Pan Chen',
    author_email='chenpan9012@gmail.com',
    description='A manage module of parade',
    url='https://github.com/cpzt/parade-manage',
    install_requires=['parade'],
    py_modules=['parade_manage'],
    zip_safe=False,
    python_requires='>=3.4',
    include_package_data=True,
    platforms=['any'],
    package_dir={"": "src"},
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Operating System :: Unix',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX'
    ]
)
