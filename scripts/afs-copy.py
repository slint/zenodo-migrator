#!/usr/bin/env python
#
# This file is part of Zenodo.
# Copyright (C) 2017 CERN.
#
# Zenodo is free software; you can redistribute it
# and/or modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 2 of the
# License, or (at your option) any later version.
#
# Zenodo is distributed in the hope that it will be
# useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Zenodo; if not, write to the
# Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston,
# MA 02111-1307, USA.
#
# In applying this license, CERN does not
# waive the privileges and immunities granted to it by virtue of its status
# as an Intergovernmental Organization or submit itself to any jurisdiction.

import json
import os
import re
from contextlib import contextmanager
from distutils.spawn import find_executable
from time import sleep

import click
import libtmux

TMUX_SESSION_NAME = 'afseos'
DEFAULT_SESSION_COUNT = 20

TMP_PASS_FILE = '/tmp/temp.pass'

EOS_DATA_DIR = 'root://eospublic.cern.ch//eos/zenodo/prod/legacydata'
EOS_FILES_DIR = EOS_DATA_DIR + '/files'
EOS_DEPOSIT_DIR = EOS_DATA_DIR + '/deposit/storage'

AFS_DATA_DIR = '/afs/cern.ch/project/zenodo/prod/var/data'
AFS_FILES_DIR = AFS_DATA_DIR + '/files'
AFS_DEPOSIT_DIR = AFS_DATA_DIR + '/deposit/storage'

COPY_CMD = u'xrdcp -r --parallel 4 {src} {dst}'
FOR_LOOP_COPY_CMD = (
    u'for d in {dirs_array}; do '
    u'xrdcp {force} -r --parallel 4 {src_dir}/$d {dst_dir}/$d; done'
)
SSH_CMD = u'sshpass -f {} ssh zenodo@lxplus'.format(TMP_PASS_FILE)


def chunkify(l, n):
    """Split a list to N equally sized chunks."""
    return [l[i::n] for i in xrange(n)]


@contextmanager
def store_login_password(password):
    with open(TMP_PASS_FILE, 'a'):
        os.utime(TMP_PASS_FILE, None)
    os.chmod(TMP_PASS_FILE, 0o600)
    with open(TMP_PASS_FILE, 'w') as fp:
        fp.write(password)
    yield
    os.unlink(TMP_PASS_FILE)


def create_panes(window, count=20):
    for _ in range(count - 1):  # 1 pane already open
        window.split_window()
        window.cmd('select-layout', 'tiled')
    return window.panes


_current_lxplus_sessions = set()


def ssh_to_lxplus(pane, unique_sessions=False):
    pane.clear()
    pane.send_keys(SSH_CMD)
    if unique_sessions:
        last_line = ''
        host_regex = r'zenodo@(lxplus\d+) ~'
        while not re.search(host_regex, last_line):
            sleep(1)
            last_line = pane.cmd('capture-pane', '-p').stdout[-1]
        res = re.search(host_regex, last_line)
        if res:
            lxplus_host = res.groups()[0]
            if lxplus_host in _current_lxplus_sessions:
                ssh_to_lxplus(pane)
            else:
                _current_lxplus_sessions.add(lxplus_host)
    else:
        sleep(2)


def create_dirs_on_eos(pane, eos_dir, dir_list, dry_run=True):
    # Launch EOS shell
    pane.send_keys(u'eos root://eospublic.cern.ch/')
    # Remove root:// prefix from path
    eos_path = eos_dir.replace('root://eospublic.cern.ch/', '')
    for d in dir_list:
        new_dir = os.path.join(eos_path, d)
        if dry_run:
            pane.send_keys(u'# mkdir -p {}'.format(new_dir))
        else:
            pane.send_keys(u'mkdir -p {}'.format(new_dir))
    pane.send_keys('exit')


def start_copy_sessions(window, eos_dir, afs_dir, dir_list, force=False,
                        create_eos_dirs=True, session_count=20, dry_run=True):
    panes = create_panes(window, session_count)
    for p in panes:
        ssh_to_lxplus(p)
    click.prompt('Press enter when all sessions are ready...', default='')
    if create_eos_dirs:
        click.echo('Creating directories on EOS...')
        create_dirs_on_eos(panes[0], eos_dir, dir_list, dry_run=dry_run)
    click.prompt('Press enter to start copying files...', default='')

    # Send the copy commands
    dir_chunks = list(chunkify(dir_list, session_count))
    assert len(dir_chunks) == len(panes)
    force = '-f' if force else ''
    for p, dirs in zip(panes, dir_chunks):
        cmd = FOR_LOOP_COPY_CMD.format(
            dirs_array=(' '.join(dirs)), src_dir=afs_dir, dst_dir=eos_dir,
            force=force)
        click.echo(u'\n{} will run: {}\n'.format(p, cmd))
        # There's a limit for the tmux's "send_keys" message size
        assert len(cmd) <= 16000
        if dry_run:
            p.send_keys(u'# {}'.format(cmd))
        else:
            p.send_keys(u'{}'.format(cmd))


def create_tmux_session(session_name):
    tmux_server = libtmux.Server()
    tmux_session = tmux_server.find_where(
        {'session_name': session_name})

    if not tmux_session:
        tmux_session = tmux_server.new_session(session_name)
    assert len(tmux_session.windows) == 1

    window = tmux_session.windows[0]
    assert len(window.panes) == 1

    click.echo(u'Launch the tmux session:\n\n')
    click.secho(u'  tmux attach -t {}\n'.format(session_name), fg='blue')
    click.prompt('Press enter to continue...', default='')
    return window


@click.command('copy-files')
@click.option('-d', '--dirs-json', type=click.File('r'), required=True,
              help='Path to JSON file with list of directories to be copied.')
@click.option('-a', '--afs-dir', required=True,
              help='Base AFS source directory.')
@click.option('-e', '--eos-dir', required=True,
              help='Base EOS destination directory.')
@click.option('-f', '--force', is_flag=True,
              help='Force-copy, ie. overwrite existing files on EOS.')
@click.option('-c', '--create-eos-dirs', is_flag=True)
@click.option('-s', '--session-name', default=TMUX_SESSION_NAME)
@click.option('-p', '--session-count', default=DEFAULT_SESSION_COUNT)
@click.option('-n', '--dry-run', is_flag=True)
@click.password_option()
def copy_files(dirs_json, afs_dir, eos_dir, force, create_eos_dirs,
               session_name, session_count, dry_run, password):
    """Copy files from AFS to EOS.

    Examples:

    copy-files\n
        -d /path/to/files-dirs.json\n
        -a /afs/cern.ch/project/zenodo/prod/var/data/files\n
        -e root://eospublic.cern.ch//eos/zenodo/prod/legacydata/files\n
        --create-eos-dirs --dry-run
    """
    assert find_executable('tmux'), 'You need to have "tmux" installed'
    assert find_executable('sshpass'), 'You need to have "sshpass" installed'

    with store_login_password(password):
        window = create_tmux_session(session_name)
        dir_list = [f.strip() for f in json.load(dirs_json)]

        start_copy_sessions(
            window=window,
            afs_dir=afs_dir,
            eos_dir=eos_dir,
            dir_list=dir_list,
            force=force,
            create_eos_dirs=create_eos_dirs,
            session_count=session_count,
            dry_run=dry_run,
        )


if __name__ == '__main__':
    copy_files()
