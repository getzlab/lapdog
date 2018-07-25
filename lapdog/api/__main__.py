import connexion
import os
from flask_cors import CORS

swagger_dir = os.path.join(
    os.path.dirname(__file__),
    'swagger'
)

def run():
    app = connexion.App('lapdog-api', specification_dir=swagger_dir)
    app.add_api('lapdog.yaml')
    CORS(app.app, origins=r'.*')
    app.app.config['storage'] = {}
    app.run(port=4201)

if __name__ == '__main__':
    run()
