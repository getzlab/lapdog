from setuptools import setup
setup(
    name = 'lapdog',
    version = '0.1.0',
    packages = ['lapdog'],
    package_data={'':['wdl_pipeline.yaml']},
    description = 'A wrapper for FISS and dalmatian',
    author = 'Broad Institute - Cancer Genome Computational Analysis',
    author_email = 'gdac@broadinstitute.org',
    long_description = 'A wrapper for FISS and dalmatian',
    entry_points = {
        'console_scripts': [
            'lapdog = lapdog.__main__:main'
        ]
    },
    install_requires = [
        'firecloud-dalmatian',
        'google-cloud-storage',
        'pyyaml',
        'agutil'
    ],
    classifiers = [
        "Programming Language :: Python :: 3",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator",
    ],
)
