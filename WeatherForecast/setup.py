from setuptools import setup, find_packages

setup(
    name="seattle_weather_pipeline",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        line.strip()
        for line in open("requirements/base.txt")
        if line.strip() and not line.startswith("#")
    ],
    author="Nandini Wadaskar",
    description="Seattle Weather Data Pipeline",
    python_requires=">=3.8",
)



#This code helps to install depedency from requirement file for the specified version