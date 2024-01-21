class TaskContext:
    __names__ = tuple()


class FlowContext:
    __active__ = None
    __names__ = tuple()

    @classmethod
    def get_current_flow(cls):
        return cls.__active__
