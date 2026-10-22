# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Host runtime effects consume a previously sealed private ownership receipt."""

import contextlib
import json
import sys
import uuid

from host_inputs import arguments, options, validate
from host_runtime import HostRuntime, require_owned


def prepare(request):
    previous = request.get('record')
    record = previous or {'version': 1, 'cluster': request['cluster'],
                          'owner': str(uuid.uuid4()), 'name': 'crowdb-' + request['cluster'] + '-host'}
    if record['version'] != 1 or record['cluster'] != request['cluster']:
        raise ValueError('host receipt identity differs from the selected cluster')
    record = dict(record, options=options(request, previous))
    runtime = HostRuntime(record['options'], record['cluster'])
    identity = runtime.identity()
    if previous and identity != record['runtime']:
        raise ValueError('runtime identity differs from the accepted host receipt')
    current = runtime.named(record['name'])
    if current:
        if not previous:
            raise ValueError('same-name host container has no accepted ownership receipt')
        require_owned(current, record)
    disks, hashes, origins = validate(record, bool(current and current['State']['Running']))
    reference = record['options']['image']
    image = runtime.inspect(reference.rsplit('@', 1)[-1] if runtime.name == 'containerd'
                            else reference, image=True)
    if (image.get('Os') != 'linux' or image.get('Architecture') != 'amd64'
            or image.get('Config', {}).get('Labels', {}).get('org.opencontainers.image.source')
            != 'https://github.com/buzzcrow/crowdb'):
        raise ValueError('host launch requires an already loaded Linux amd64 CROWDB node image')
    runtime_image = (record['options']['image'].rsplit('@', 1)[-1]
                     if runtime.name == 'containerd' else image['Id'])
    current_record = dict(record, runtime=identity, runtime_image=runtime_image,
                          image_id=image['Id'], disks=disks, file_hashes=hashes, origins=origins)
    if previous and current_record != previous:
        raise ValueError('host image, policy, credentials, media or address differ from accepted inputs')
    return current_record


def verify(runtime, current, record):
    require_owned(current, record)
    if current['State']['Running']:
        runtime.verify(current['Id'], record['options']['cpus'], record['options']['memory_mib'], True)


def run(request):
    record = prepare(request)
    if request.get('record') != record:
        raise ValueError('seal accepted host inputs before requesting runtime effects')
    runtime = HostRuntime(record['options'], record['cluster'])
    runtime.check(record)
    current = runtime.named(record['name'])
    if current:
        verify(runtime, current, record)
        if not current['State']['Running']:
            runtime.check(record)
            runtime.invoke('start', current['Id'])
    else:
        args = arguments(record)
        args.image = record['runtime_image']
        runtime.check(record)
        # Runtime failure diagnostics belong on stderr; stdout remains JSON.
        with contextlib.redirect_stdout(sys.stderr):
            runtime.create(args)
    current = runtime.inspect(record['name'])
    verify(runtime, current, record)
    return {'container_id': current['Id']}


def stop(request):
    record = request['record']
    runtime = HostRuntime(record['options'], record['cluster'])
    runtime.check(record)
    current = runtime.named(record['name'])
    if current:
        require_owned(current, record)
        if current['State']['Running']:
            runtime.check(record)
            runtime.invoke('stop', current['Id'])
    return {'data_preserved': True}


def main():
    request = json.load(sys.stdin)
    action = request['operation']
    handler = {'prepare': prepare, 'run': run, 'stop': stop}[action]
    print(json.dumps(handler(request)))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, RuntimeError, KeyError, TypeError) as error:
        print(str(error), file=sys.stderr)
        sys.exit(2)
