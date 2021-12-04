

def main():
    x = 1
    print('test12 begins')
    print(12)
    print('test12 ends')
    virtual_return_register_1 = 1
    print('test34 begins')
    x = (x * 34)
    print((x - 34))
    print('test34 ends')
    virtual_return_register_2 = (x + 1)
    print('test56 begins')
    print('test12 begins')
    print(12)
    print('test12 ends')
    virtual_return_register_0 = 1
    test56_y = virtual_return_register_0
    print(test56_y)
    print('test56 ends')
    virtual_return_register_3 = 5
    y = ((virtual_return_register_1 + virtual_return_register_2) + virtual_return_register_3)
    print(y)

main()