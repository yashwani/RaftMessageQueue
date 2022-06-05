from threading import Timer
import random


class Timers:
    def __init__(self, lo, hi, on_expire_fn, args):
        self.lo = lo
        self.hi = hi
        self.on_expire_fn = on_expire_fn
        self.args = args
        intvl = random.randint(lo, hi) / 1000
        self.timer = Timer(interval=intvl, function=on_expire_fn, args=args)


    def create_timer(self):
        self.timer = None
        intvl = random.randint(self.lo, self.hi) / 1000
        self.timer = Timer(interval=intvl, function=self.on_expire_fn, args=self.args)


    def restart(self):
        if self.timer.is_alive():
            self.cancel()
        self.create_timer()
        self.start()

    def start(self):
        self.timer.start()

    def cancel(self):
        self.timer.cancel()


