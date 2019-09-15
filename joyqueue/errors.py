from __future__ import absolute_import


class JoyqueueError(RuntimeError):
    retriable = False
    # whether metadata should be refreshed on error
    invalid_metadata = False

    def __str__(self):
        if not self.args:
            return self.__class__.__name__
        return '{0}: {1}'.format(self.__class__.__name__,
                                 super(JoyqueueError, self).__str__())


class BufferUnderflowError(JoyqueueError):
    pass
