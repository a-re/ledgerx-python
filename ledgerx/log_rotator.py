import logging
import logging.handlers
from multiprocessing import Process
import os
import gzip

class GZipRotator:
    old_p = None
    def mp__call__(self, dest):
        logging.info(f"Starting compression of {dest}")
        f_in = open(dest, 'rb')
        f_out = gzip.open("%s.gz" % dest, 'wb')
        f_out.writelines(f_in)
        f_out.close()
        f_in.close()
        os.remove(dest)
        logging.info(f"Completed rotation to {dest}.gz")

    def __call__(self, source, dest):
        logging.info(f"rotating logs from {source} to {dest}")
        if self.old_p is not None:
            logging.info(f"joining old compression proc={self.old_p}")
            self.old_p.join()
            self.old_p = None
        os.rename(source, dest)
        p = Process(target=self.mp__call__, args=(dest,))
        p.start()
        self.old_p = p
        logging.info(f"running compression in proc={p}")

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
