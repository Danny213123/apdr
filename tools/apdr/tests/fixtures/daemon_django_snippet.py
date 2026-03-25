"""
DaemonCommand - Django Management Command for starting a daemon.

Use::

    from daemonextension import DaemonCommand
    from django.conf import settings

    class Command(DaemonCommand):
        def handle_daemon(self, *args, **options):
            from flopsy import Connection, Consumer
            consumer = Consumer(connection=Connection())
"""

from django.core.management.base import BaseCommand
from optparse import make_option

import daemon
from daemon import pidlockfile

class DaemonCommand(BaseCommand):
    help = 'Create a daemon'

    def handle(self, *args, **options):
        context = daemon.DaemonContext()
        pidfile = options.get('pidfile')
        if pidfile is not None:
            context.pidfile = pidlockfile.PIDLockFile(pidfile)
        context.open()
        self.handle_daemon(*args, **options)

    def handle_daemon(self, *args, **options):
        raise NotImplementedError()
