from setuptools import setup, find_packages

setup(
    name='sqlalchemy-jupyter-content-manager',
    install_requires=[
        'sqlalchemy'
    ],
    extras_require={
        'pg': [
            'psycopg2-binary'
        ]
    },
    packages=find_packages()
)

