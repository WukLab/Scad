import time

def main(_, action):
    time.sleep(1)
    action.profile(0)
    a = list(range(1024 * 1024 * 64))
    action.profile(1)
    time.sleep(10)
    action.profile(2)

