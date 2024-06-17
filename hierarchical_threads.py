import threading

def get_thread_hierarchy(thread=None):
    """
    """
    result = [thread or threading.current_thread()]
    while getattr(result[-1], "parent", None) is not None:
        result.append(result[-1].parent)
    if result[-1] != threading.main_thread():
        raise RuntimeError(f"Thread '{result[-1]}' was not created as hierarchical thread!")
    return result

# Patch constructor of threading.Thread
init_backup = threading.Thread.__init__
def custom_init(self, *args, **kwargs):
    self.parent = threading.current_thread()
    init_backup(self, *args, **kwargs)
threading.Thread.__init__   = custom_init
threading.Thread.hierarchy  = property(get_thread_hierarchy)

