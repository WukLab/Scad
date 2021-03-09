def test12():
    print("test12 begins")
    print("test12 ends")
    return 3
def test34():
    print("test34 begins")
    print("test34 ends")
    return 1
def test56():
    print("test56 begins")
    print("test56 ends")
    return 2
def test78(x):
    x.append(1)
    return 1
def main():
    i = 227 % 27
    if i % 4 == 1:
        test12()
    elif i % 4 == 2:
        test34()
    elif i % 4 == 3:
        test56()
    else:
        test12()
    # testing exec order
    x = []
    if test78(x) > 1:
        print(1)
    elif test78(x) > 2:
        print(2)
    elif len(x) > 1:
        print(3)

main()