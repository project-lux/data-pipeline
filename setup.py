from setuptools import setup, find_packages

setup(
    name="lux-pipeline",
    version="0.0.3",
    packages=find_packages(),
    install_requires=[
        "requests",
        "redis",
        "psycopg2-binary",
        "cromulent>=1.0.1",
        "requests_toolbelt",
        "ujson",
        "SPARQLWrapper",
        "dateutils",
        "edtf",
        "dateparser",
        "shapely",
        "lxml",
        "jsonschema",
        "lmdbm>=0.0.6",
        "sqlitedict",
        "numpy",
        "bs4",
        "python-dotenv",
        "pyluach",
        "PyGithub",
        "rich"
    ],
    entry_points={
        'console_scripts': [
            'lux = lux_pipeline.cli.entry:main',
        ]
    }
)
