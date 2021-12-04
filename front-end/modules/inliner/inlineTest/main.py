
def hello(x):
    -(x + add_stuff(0, 1))

def add_stuff(x, y):
    return x + y, x - hello(x), 0

def main():
    i = 0
    y = 0
    print(y)
    while i < 10:
        y,x,z = add_stuff(i, i * 3)
        i = i + 1

main()