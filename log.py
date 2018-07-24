# -*- coding: utf-8 -*-

import logging
import sys
from datetime import datetime

try:
    import colorama
except ImportError:
    colorama = None

try:
    import curses  # type: ignore
except ImportError:
    curses = None

try:
    import codecs
except ImportError:
    codecs = None

unicode_type = unicode
basestring_type = basestring


def init_logger(logger=None, level="INFO", path="./"):
    if not logger:
        logger = logging.getLogger()

    logger.setLevel(getattr(logging, level.upper()))
    channel = logging.StreamHandler()
    channel.setFormatter(LogFormatter())
    logger.addHandler(channel)

    channel = DaemonFileLogHandler(path)
    channel.setFormatter(LogFormatter())
    logger.addHandler(channel)


def _stderr_supports_color():
    try:
        if hasattr(sys.stderr, 'isatty') and sys.stderr.isatty():
            if curses:
                curses.setupterm()
                if curses.tigetnum("colors") > 0:
                    return True
            elif colorama:
                if sys.stderr is getattr(colorama.initialise, 'wrapped_stderr',
                                         object()):
                    return True
    except Exception:
        # Very broad exception handling because it's always better to
        # fall back to non-colored logs than to break at startup.
        pass
    return False


def _safe_unicode(value):
    if isinstance(value, unicode):
        return value

    try:
        return value.decode("utf-8")
    except UnicodeDecodeError:
        return repr(value)


class DaemonFileLogHandler(logging.FileHandler):

    # _LOG_FILEFORMAT = '%Y%m%d%H'
    _LOG_FILEFORMAT = '%Y%m%d'

    def __init__(self, filename, mode='a', encoding=None, delay=0):
        super(DaemonFileLogHandler, self).__init__(filename, mode, encoding, delay)

    def get_cur_filename(self):
        t = datetime.now()
        newlogfile = "%s/%s.log" % (self.baseFilename, t.strftime(self._LOG_FILEFORMAT))
        return newlogfile

    def _open(self):
        if self.encoding is None:
            stream = open(self.get_cur_filename(), self.mode)
        else:
            stream = codecs.open(self.get_cur_filename(), self.mode, self.encoding)
        return stream

    def _get_stream(self):
        if self.stream is None:
            self.stream = self._open()
        else:
            cur_filename = self.get_cur_filename()
            if cur_filename != self.stream.name:
                self.stream = self._open()

        return self.stream

    def emit(self, record):
        try:
            self._get_stream()
        except:
            return
        return logging.StreamHandler.emit(self, record)


class LogFormatter(logging.Formatter):
    DEFAULT_FORMAT = '%(color)s[%(levelname)1.1s %(asctime)s %(module)s:%(lineno)d]%(end_color)s %(message)s'
    DEFAULT_DATE_FORMAT = '%y%m%d %H:%M:%S'
    DEFAULT_COLORS = {
        logging.DEBUG: 4,       # Blue
        logging.INFO: 2,        # Green
        logging.WARNING: 3,     # Yellow
        logging.ERROR: 1,       # Red
    }

    def __init__(self, fmt=DEFAULT_FORMAT, datefmt=DEFAULT_DATE_FORMAT,
                 style='%', color=True, colors=DEFAULT_COLORS):

        logging.Formatter.__init__(self, datefmt=datefmt)
        self._fmt = fmt

        self._colors = {}
        if color and _stderr_supports_color():
            if curses is not None:
                fg_color = (curses.tigetstr("setaf") or
                            curses.tigetstr("setf") or "")
                if (3, 0) < sys.version_info < (3, 2, 3):
                    fg_color = unicode_type(fg_color, "ascii")

                for levelno, code in colors.items():
                    self._colors[levelno] = unicode_type(curses.tparm(fg_color, code), "ascii")
                self._normal = unicode_type(curses.tigetstr("sgr0"), "ascii")
            else:
                for levelno, code in colors.items():
                    self._colors[levelno] = '\033[2;3%dm' % code
                self._normal = '\033[0m'
        else:
            self._normal = ''

    def format(self, record):
        try:
            message = record.getMessage()
            assert isinstance(message, basestring_type)  # guaranteed by logging
            record.message = _safe_unicode(message)
        except Exception as e:
            record.message = "Bad message (%r): %r" % (e, record.__dict__)

        record.asctime = self.formatTime(record, self.datefmt)

        if record.levelno in self._colors:
            record.color = self._colors[record.levelno]
            record.end_color = self._normal
        else:
            record.color = record.end_color = ''

        formatted = self._fmt % record.__dict__

        if record.exc_info:
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            lines = [formatted.rstrip()]
            lines.extend(_safe_unicode(ln) for ln in record.exc_text.split('\n'))
            formatted = '\n'.join(lines)
        return formatted.replace("\n", "\n    ")


if __name__ == "__main__":
    logger = logging.getLogger()
    init_logger(logger, level="DEBUG")
    logger.debug("test")
    logger.error("error")
