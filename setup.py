from setuptools import setup
from lapdog import __version__
setup(
    name = 'lapdog',
    version = __version__,
    packages = [
        'lapdog',
        'lapdog.api',
        'lapdog.cloud'
    ],
    package_data={
        '':[
            'cromwell/wdl_pipeline.yaml',
            'cromwell/LICENSE',
            'cromwell/README.md',
            'vue/.babelrc',
            'vue/index.html',
            'vue/package.json',
            'vue/webpack.config.js',
            'vue/src/main.js',
            'vue/src/App.vue',
            'vue/src/Pages/*.vue',
            'api/swagger/lapdog.yaml',
            'wdl_pipeline.yaml'
        ],
    },
    description = 'A relaxed wrapper for FISS and dalmatian',
    url = 'https://github.com/broadinstitute/lapdog',
    author = 'Aaron Graubert - Broad Institute - Cancer Genome Computational Analysis',
    author_email = 'aarong@broadinstitute.org',
    long_description = 'A wrapper for FISS and dalmatian',
    entry_points = {
        'console_scripts': [
            'lapdog = lapdog.__main__:main'
        ]
    },
    install_requires = [
        'firecloud>=0.16.9',
        'firecloud-dalmatian>=0.0.4',
        'google-cloud-storage>=1.13.2',
        'PyYAML==4.2b1',
        'agutil>=4.0.2',
        'Flask-Cors==3.0.6',
        'crayons==0.1.2',
        'connexion==1.5.3',
        'oauth2client',
        'requests>=2.18.0',
        'googleapis-common-protos>=1.5.0',
        'google-auth>=1.4.0',
        'google-cloud-kms==0.2.1',
        'cryptography>=2.1.0'
    ],
    classifiers = [
        "Programming Language :: Python :: 3",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator",
    ],
)
