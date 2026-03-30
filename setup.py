from setuptools import find_namespace_packages, setup

setup(
    name="lux-pipeline",
    version="0.0.4",
    packages=find_namespace_packages(include=["lux_pipeline"]),
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
        "rich",
    ],
    entry_points={
        "console_scripts": [
            "lux = lux_pipeline.cli.entry:main",
        ]
    },
)
