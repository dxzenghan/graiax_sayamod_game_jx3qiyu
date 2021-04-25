import asyncio
from asyncio import AbstractEventLoop
import threading
from threading import Lock, Thread
from queue import Queue
from typing import Iterable, Tuple, Union, Dict


class LoopManagerMeta(type):
    _instances = {}
    _lock: Lock = Lock()

    def __call__(cls, *args, **kwargs):
        with cls._lock:
            if cls not in cls._instances:
                instance = super().__call__(*args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]


class LoopManager(metaclass=LoopManagerMeta):
    _loops = {}  # {"ThreadName": loop object}

    def __init__(self) -> None:
        self._loops.update(
            {threading.current_thread().getName(): asyncio.get_event_loop()}
        )

    def start_new_loop(self, thread_name):
        new_loop = asyncio.new_event_loop()

        def new_thread(new_loop: AbstractEventLoop):
            asyncio.set_event_loop(new_loop)
            new_loop.run_forever()

        new_thread = Thread(
            target=new_thread,
            args=(new_loop,),
            name=thread_name,
        )
        new_thread.setDaemon(True)
        new_thread.start()
        return new_thread, new_loop

    def new_event_loop(
        self, thread_name: Union[str, Iterable[str]] = None
    ) -> AbstractEventLoop:
        if isinstance(thread_name, str):
            new_thread, new_loop = self.start_new_loop(thread_name)
            self._loops.update({new_thread.getName(): new_loop})
            return new_loop
        elif isinstance(thread_name, Iterable):
            new_loops = {
                new_thread.getName(): new_loop
                for new_thread, new_loop in (
                    self.start_new_loop(single_name) for single_name in thread_name
                )
            }
            self._loops.update(new_loops)
            return new_loops
        else:
            raise TypeError("Param thread_name must be str or iterable type.")

    def get_event_loop(
        self, thread_name=None
    ) -> Union[
        Dict[str, Tuple[AbstractEventLoop, Queue]], Tuple[AbstractEventLoop, Queue]
    ]:
        if thread_name is None:
            return self._loops
        else:
            return self._loops.get(thread_name)

    def create_task(self, task, thread_name):
        asyncio.run_coroutine_threadsafe(task, self._loops.get(thread_name))
