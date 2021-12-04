def test12():
    print("test12 begins")
    print(12)
    print("test12 ends")
    return 1

def test34(ooo):
    print("test34 begins")
    ooo = ooo * 34
    print(ooo - 34)
    print("test34 ends")
    return ooo + 1

def test56():
    print("test56 begins")
    y = test12()
    print(y)
    print("test56 ends")
    return 5

# testing basic function call with basic arguments
def main():
    x = 1
    y = test12() + test34(x) + test56()
    print(y)

main()