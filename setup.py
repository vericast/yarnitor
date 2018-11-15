import versioneer
from setuptools import setup, find_packages

setup_args = dict(
    name='yarnitor',
    version=versioneer.get_version(),
    entry_points={
        'console_scripts': [
            'yarnitor-background-worker = yarnitor.background.worker:main'
        ]
    },
    cmdclass=versioneer.get_cmdclass(),
    author='MaxPoint Interactive',
    license='BSD 3-clause',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'flask',
        'flask-caching',
        'flask-redis',
        'requests',
    ]
)

if __name__ == '__main__':
    setup(**setup_args)
