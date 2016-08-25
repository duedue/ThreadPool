# ThreadPool is a simple thread pool
#
# Copyright (C) 2012 Yummy Bian <yummy.bian#gmail.com>
#
# under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# ThreadPool is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.";
#

import Queue
import traceback
import threading

class ResultPool(object):
    """Storage task result pool."""
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(ResultPool, cls).__new__(cls, *args, **kwargs)
            cls._instance.pool = {}

        return cls._instance

    @classmethod
    def list(cls):
        return cls._instance.pool

    @classmethod
    def get(cls, key):
        if key in cls._instance.pool:
            return cls._instance.pool[key]

        return None

    @classmethod
    def set(cls, key, val):
        cls._instance.pool[key] = val

    @classmethod
    def clear(cls):
        cls._instance.pool = {}


class Worker(threading.Thread):
    """Routines for work thread."""
    def __init__(self, in_queue):
        """Initialize and launch a work thread,
        in_queue which tasks in it waiting for processing,
        result which store tasks' result.
        """
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self.in_queue = in_queue
        self.result = ResultPool()
        self.start()

    def run(self):
        while True:
            # Processing tasks in the in_queue until command is stop.
            command, task_name, callback, args, kwds = self.in_queue.get()
            try:
                if command == 'stop':
                    break
                if command != 'process':
                    raise ValueError('Unknown command %r' % command)

                self.result.set(task_name, callback(*args, **kwds))
            except:
                self.result.set(task_name, {'err': traceback.format_exc()})

    def dismiss(self):
        command = 'stop'
        self.in_queue.put((command, None, None, None))


class ThreadPool():
    """Manager thread pool."""
    max_threads = 32

    def __init__(self, num_threads, pool_size=0):
        """Spawn num_threads threads in the thread pool,
        and initialize three queues.
        """
        # pool_size = 0 indicates buffer is unlimited.
        self.num_threads = ThreadPool.max_threads \
            if num_threads > ThreadPool.max_threads \
            else num_threads
        self.in_queue = Queue.Queue(pool_size)
        self.result = ResultPool()

        self.init_workers()

    def init_workers(self):
        self.workers = {}

        for i in range(self.num_threads):
            worker = Worker(self.in_queue)
            self.workers[i] = worker

    def restart(self):
        self.destroy()
        self.init_workers()

    def add_task(self, task_name, callback, *args, **kwds):
        command = 'process'
        self.result.set(task_name, None)
        self.in_queue.put((command, task_name, callback, args, kwds))

    def get_task_result(self, task_name):
        return self.result.get(task_name)

    def get_all_task_result(self):
        return self.result.list()

    def destroy(self):
        if hasattr(self, 'workers'):
            # Clear result pool.
            self.result.clear()

            # order is important: first, request all threads to stop...:
            for i in self.workers:
                self.workers[i].dismiss()
            # ...then, wait for each of them to terminate:
            for i in self.workers:
                self.workers[i].join()
            # clean up the workers from now-unused thread objects
            del self.workers
