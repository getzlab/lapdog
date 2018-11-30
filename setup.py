from setuptools import setup
from lapdog import __version__
setup(
    name = 'lapdog',
    version = __version__,
    packages = [
        'lapdog',
        'lapdog.api'
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
    author = 'Aaron Graubert - Broad Institute - Cancer Genome Computational Analysis',
    author_email = 'aarong@broadinstitute.org',
    long_description = 'A wrapper for FISS and dalmatian',
    entry_points = {
        'console_scripts': [
            'lapdog = lapdog.__main__:main'
        ]
    },
    install_requires = [
        'firecloud-dalmatian',
        'google-cloud-storage>=1.9.0',
        'pyyaml',
        'agutil',
        'flask_cors',
        'crayons',
        'connexion',
        'oauth2client',
        'requests'
    ],
    classifiers = [
        "Programming Language :: Python :: 3",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator",
    ],
)
