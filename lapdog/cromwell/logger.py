import sys
import subprocess
import time
import os
import tempfile
import requests

last_call = None

abort_path = os.path.join(
    os.path.dirname(os.path.dirname(sys.argv[1])),
    'abort-key'
)

def abort(handle):
    from cryptography.exceptions import InvalidSignature
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec, padding, utils
    from hashlib import sha256
    from google.cloud import kms_v1 as kms

    subprocess.check_call('gsutil cp %s abort.key' % abort_path)
    with open('abort.key') as r:
        code = r.read()

    try:
        serialization.load_pem_public_key(
            kms.KeyManagementServiceClient().get_public_key(
                'projects/%s/locations/us/keyRings/lapdog/cryptoKeys/lapdog-sign/cryptoKeyVersions/1' % os.environ.get('LAPDOG_PROJECT')
            ).pem.encode('ascii'),
            default_backend()
        ).verify(
            code,
            sha256(os.environ.get('LAPDOG_SUBMISSION_ID').encode()).digest(),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=32
            ),
            utils.Prehashed(hashes.SHA256())
        )
        handle.write('<<<ABORT KEY DETECTED>>>')
        for workflow in requests.get('http://localhost:8000/api/workflows/v1/query').json():
            handle.write(requests.post('http://localhost:8000/api/workflows/v1/%s/abort' % workflow['id']).text())
        handle.write("<<<ABORTED>>>")
        subprocess.check_call('gsutil cp %s %s' % (handle.name, sys.argv[1]), shell=True)
        sys.exit("Aborted")
    except InvalidSignature:
        handle.write('<<<INVALID ABORT KEY>>>')


def throttle(n):
    def wrapper(func):
        def call(*args, **kwargs):
            global last_call
            if last_call is None or (time.time() - last_call) > n:
                last_call = time.time()
                return func(*args, **kwargs)
            # Abort
        return call
    return wrapper


def _flush(handle):
    handle.flush()
    subprocess.check_call('gsutil cp %s %s' % (handle.name, sys.argv[1]), shell=True)
    if subprocess.call('gsutil ls %s' % abort_path, shell=True) == 0:
        # abort key located
        abort(handle)
    return os.path.getsize(handle.name)

flush = throttle(120)(_flush)
flush_slow = throttle(300)(_flush)
flush_rare = throttle(3600)(_flush)

def write(handle, volume, line):
    handle.write(line.rstrip() + '\n')
    if volume > 1073741824: # 1 Gib
        upload = flush_rare(handle)
    elif volume > 52428800: # 50 Mib
        upload = flush_slow(handle)
    else:
        upload = flush(handle)
    return volume + (upload if upload is not None else 0)

volume = 0
with tempfile.NamedTemporaryFile('w') as tmp:
    while True:
        volume = write(tmp, volume, raw_input()) # Dumb, but will crash when stdin closes
        tmp.write('\n(VOLUME %d)\n' % volume)
