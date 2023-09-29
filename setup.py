from setuptools import setup

setup(
    name='lapinmq',
    version='0.1.0',    
    description='Utilities for RabbitMQ following best practices',
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
