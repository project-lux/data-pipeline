from setuptools import setup, find_packages

setup(
    name="lux-pipeline",
    version="0.0.1",
    packages=find_packages(),
    install_requires=[
        "requests",
        "tqdm"
    ],
    entry_points={
        'console_scripts': [
            'lux = lux_pipeline.cli.entry:main',
        ]
    }
)
