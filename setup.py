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
    license='MaxPoint Internal',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'flask==0.11.*',
        'flask-cache',
        'flask-redis==0.3.0',
        'requests',
    ]
)

if __name__ == '__main__':
    setup(**setup_args)
