import connexion
import os
import sys
import subprocess
import argparse
from flask_cors import CORS

swagger_dir = os.path.join(
    os.path.dirname(__file__),
    'swagger'
)

vue_dir = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    'vue'
)


def run(args):
    if args.install:
        print("Installing UI Dependencies")
        return subprocess.run(
            'npm install',
            shell=True,
            executable='/bin/bash',
            preexec_fn=lambda :os.chdir(vue_dir)
        ).returncode
    app = connexion.App('lapdog-api', specification_dir=swagger_dir)
    app.add_api('lapdog.yaml')
    CORS(app.app, origins=r'.*')
    app.app.config['storage'] = {}
    if args.vue:
        subprocess.Popen(
            'npm run dev',
            shell=True,
            executable='/bin/bash',
            preexec_fn=lambda :os.chdir(vue_dir)
        )
    app.run(port=4201)

if __name__ == '__main__':
    parser = argparse.ArgumentParser('lapdog-ui')
    parser.add_argument(
        '-v', '--vue',
        action='store_true',
        help="Launch the vue UI"
    )
    parser.add_argument(
        '--install',
        action='store_true',
        help="Installs the node dependencies to run the UI"
    )
    run(parser.parse_args())
