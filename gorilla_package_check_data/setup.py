
from setuptools import setup, find_packages

setup(
    name="gorilla_package_check_data",
    version="0.1",
    packages=find_packages(),
    install_requires=[],  # Add any dependencies if required
    author="Vainqueur KILINDO BULAMBO",
    author_email="vbulambo@gorillafund.org",
    description="This package check gorilla tracking and monitoring data before to store them in Psql database",
    long_description="This package check gorilla tracking and monitoring data before to store them in Psql database",
    long_description_content_type="text/markdown",
    url="https://github.com/VainqueurGithub/gorilla_package_check_data.git",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
