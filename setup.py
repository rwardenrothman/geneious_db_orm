from setuptools import setup

setup(
    name='GeneiousDB',
    version='2.3',
    packages=['GeneiousDB'],
    url='',
    license='',
    author='Rob Warden-Rothman',
    author_email='rob.wr@grobio',
    description='simple interface to the geneious shared database',
    install_requires=[
        'SQLAlchemy',
        'typer',
        'xmltodict',
        'biopython',
        'tqdm',
        'psycopg2-binary',
        'asyncpg'
    ]
)
