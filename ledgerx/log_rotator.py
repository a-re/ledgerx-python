import logging
import logging.handlers
from multiprocessing import Process
import threading
import os
import gzip

logger = logging.getLogger(__name__)
class GZipRotator:
    global_lock = threading.RLock()
    old_p = dict()
    # Forbidden to log within this routine in another Process!!
    def mp__call__(self, dest):
        f_in = open(dest, 'rb')
        f_out = gzip.open("%s.gz" % dest, 'wb')
        f_out.writelines(f_in)
        f_out.close()
        f_in.close()
        os.remove(dest)

    # logging within here causes recursion problems...
    def __call__(self, source, dest):
        # prevent multiple threads from rotating the same log
        p = None
        with self.global_lock:
            if not os.path.exists(source):
                return
            os.rename(source, dest)
            p = Process(target=self.mp__call__, args=(dest,))
        if p is not None:
            p.start()
            if source in self.old_p:
                old_p = self.old_p[source]
                old_p.join()
                del self.old_p[source]
            self.old_p[source] = p

    @staticmethod
    def getLogger(log_logger, filename, format='%(asctime)s\t%(message)s', level=logging.DEBUG, when='H', interval=8, atTime=None):
        formatter = logging.Formatter(format)
        log = logging.handlers.TimedRotatingFileHandler(filename=filename, when=when, interval=interval, atTime=None)
        log.setFormatter(formatter)
        log.rotator = GZipRotator()
        if level is not None:
            log.setLevel(level)
            log_logger.setLevel(level)
        log_logger.addHandler(log)
        return log_logger
