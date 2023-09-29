import os
from setuptools import setup

def read_version():
    with open("lapinmq/version", "r") as f:
        return f.read().strip("\n")

def check_tag_version():
    tag = os.getenv("GITHUB_REF")
    expected_version = read_version()
    if tag != f"refs/tags/{expected_version}":
        raise Exception(f"Tag '{tag}' does not match the expected "
                        f"version '{expected_version}'")

with open("README.md", "r") as fh:
    long_description = fh.read()

check_tag_version()

setup(
    name='lapinmq',
    version=read_version(),
    description='Utilities for RabbitMQ following best practices',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/madhur-tandon/lapinmq',
    author='Madhur Tandon',
    author_email='madhurtandon23@gmail.com',
    license='BSD 2-clause',
    install_requires=['pika==1.3.2'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
