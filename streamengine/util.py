import venusian


class Registry(object):
    """venusian registry class
    
    Arguments:
        None
    """

    def __init__(self):
        self.registered = []

    def add(self, name, obj, config):
        self.registered.append((name, obj, config))


"""
    Decorator classes (app.agent, app.timer, app.on_start, ...) - used in the script and scanned by venusian lib.
    """

class Agent(object):
    def __init__(self,
                    stream,
                    model=None,
                    group=None,
                    concurrency=None,
                    processes=None):
        self.decorator_type = "agent"
        self.config = {
            "stream": stream,
            "model": model,
            "group": group,
            "concurrency": concurrency,
            "processes": processes
        }

    def __call__(self, wrapped):
        me = self

        class Wrapper(object):
            def __init__(self, wrapped_func):
                self.callback = wrapped

            def on_scan(self, scanner, name, obj):
                def decorated(*args, **kwargs):
                    v = wrapped_func(*args, **kwargs)
                    return v

                #self.callback = decorated
                scanner.registry.add(me.decorator_type, self.callback,
                                     me.config)

            def __call__(self, *args, **kwargs):
                return self.callback(*args, **kwargs)

        w = Wrapper(wrapped)
        venusian.attach(w, w.on_scan)
        return w

class Timer(object):
    def __init__(self, value):
        self.decorator_type = "timer"
        self.config = {"value": value}

    def __call__(self, wrapped):
        me = self

        class Wrapper(object):
            def __init__(self, wrapped_func):
                self.callback = wrapped

            def on_scan(self, scanner, name, obj):
                def decorated(*args, **kwargs):
                    v = wrapped_func(*args, **kwargs)
                    return v

                #self.callback = decorated
                scanner.registry.add(me.decorator_type, self.callback,
                                     me.config)

            def __call__(self, *args, **kwargs):
                return self.callback(*args, **kwargs)

        w = Wrapper(wrapped)
        venusian.attach(w, w.on_scan)
        return w

class Event(object):
    def __init__(self, val):
        self.decorator_type = val

    def __call__(self, wrapped):
        me = self

        class Wrapper(object):
            def __init__(self, wrapped_func):
                self.callback = wrapped

            def on_scan(self, scanner, name, obj):
                def decorated(*args, **kwargs):
                    v = wrapped_func(*args, **kwargs)
                    return v

                #self.callback = decorated
                scanner.registry.add(me.decorator_type, self.callback, [])

            def __call__(self, *args, **kwargs):
                return self.callback(*args, **kwargs)

        w = Wrapper(wrapped)
        venusian.attach(w, w.on_scan)
        return w