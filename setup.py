import setuptools


def get_version():
    version = {}
    with open("version.py") as f:
        for line in f:
            if line.startswith("__version__"):
                delim = '"' if '"' in line else "'"
                version = line.split(delim)[1]
                break
    return version


setuptools.setup(
    name="AWS-Data-Lakehouse-Pipeline-End-to-End-",
    version=get_version(),
    author="Luis Bleda Torres",
    author_email="luisbledatorres@hotmail.com",
    description="End-to-end data pipeline built on AWS simulating.",
    packages=setuptools.find_packages(),
    python_requires=">=3.6",
)
