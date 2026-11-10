#!/usr/bin/env python3
"""Проверки для Linux: make check. Все результаты создаются во временной папке."""

import os
from pathlib import Path
import re
import signal
import subprocess
import tempfile
import time

os.chdir(Path(__file__).resolve().parent)


def run_case(mode, fixture, directory, interrupt=False):
    values = list(map(int, Path(fixture).read_text().split()))
    actors, passengers, capacity = values[1] + values[3], values[3], values[2]
    independent = mode.endswith('server')
    name = mode + '-' + Path(fixture).stem
    session = 'test_' + str(os.getpid()) + '_' + name
    output = directory / (name + '.out')
    errors = directory / (name + '.err')
    log = directory / (name + '.log')
    processes = []

    def start(binary, arguments, out, err):
        with out.open('w') as stdout, err.open('w') as stderr:
            process = subprocess.Popen(['./build/' + binary, *arguments],
                stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr,
                start_new_session=True)
        processes.append(process)
        return process

    def until(process, text):
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if text in output.read_text():
                return
            if process.poll() is not None:
                break
            time.sleep(.01)
        raise AssertionError('Не дождались события: ' + text)

    try:
        arguments = [fixture, session, str(log)] if independent else [fixture, str(log)]
        server = start('elevator-' + mode, arguments, output, errors)
        invalid = fixture == 'invalid.in'
        if independent and not invalid:
            until(server, 'Готово:')
            for actor in range(1, actors + 1):
                start('elevator-' + mode.replace('server', 'client'),
                      [fixture, session, str(actor)],
                      directory / (name + str(actor) + '.out'),
                      directory / (name + str(actor) + '.err'))
        if interrupt:
            until(server, 'начало движения')
            server.send_signal(signal.SIGINT)
        expected = 1 if invalid else 130 if interrupt else 0
        assert server.wait(timeout=15) == expected, errors.read_text()
        for process in processes[1:]:
            assert process.wait(timeout=6) == (143 if interrupt else 0)
        if invalid:
            assert 'Входные данные' in errors.read_text()
        else:
            text = output.read_text()
            assert not errors.read_text(), errors.read_text()
            assert 'Итог:' in text and 'Итог:' in log.read_text()
            if not interrupt:
                assert f'доставлено {passengers}/{passengers}' in text
                routes = list(zip(values[12 + values[1]::2], values[13 + values[1]::2]))
                delivered = re.findall(r'Пассажир (\d+): высадка завершена, доставлен на этаж (\d+)', text)
                assert len(delivered) == len({person for person, _ in delivered}) == passengers
                for person, floor in delivered:
                    assert int(floor) == routes[int(person) - 1][1]
            for load, limit in re.findall(r'занято (\d+)/(\d+)', text):
                assert 0 < int(load) <= int(limit) == capacity
            if interrupt:
                assert 'последнее подтверждённое состояние' in text
        prefix = ('acos03_mq_' if mode == 'mq-server' else 'acos03_run_') + session
        if not independent:
            prefix = 'acos03_' + str(server.pid)
        for folder in ['/dev/shm', '/dev/mqueue']:
            if Path(folder).exists():
                leftovers = [p.name for p in Path(folder).iterdir()
                    if p.name == prefix or p.name.startswith(prefix + '_') or
                    p.name.startswith('sem.' + prefix + '_')]
                assert not leftovers, 'Остались объекты IPC: ' + str(leftovers)
        print('PASS:', name, flush=True)
    except Exception:
        print('FAIL:', name, '\n', output.read_text()[-2000:] if output.exists() else '',
              '\n', errors.read_text() if errors.exists() else '', flush=True)
        raise
    finally:
        for process in processes:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
        for process in processes:
            try:
                process.wait(timeout=6)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()


with tempfile.TemporaryDirectory(prefix='acos03-tests-') as temporary:
    directory = Path(temporary)
    for mode in ['unnamed', 'named', 'server', 'mq-server']:
        for fixture in ['basic.in', 'group.in', 'nearest.in', 'empty.in', 'invalid.in']:
            run_case(mode, fixture, directory)
        run_case(mode, 'interrupt.in', directory, interrupt=True)
print('Все 24 проверки пройдены.')
